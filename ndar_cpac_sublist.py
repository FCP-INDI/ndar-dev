# ndar_cpac_sublist.py
#
# Author: Daniel Clark, 2014

'''
This module contains functions which assist in building a CPAC-
compatible subject list from a miNDAR database

Usage:
    python ndar_cpac_sublist.py <inputs_dir> <study_name> <creds_path> <sublist_yaml>
'''

# Get S3 image filepath
def add_s3_path(cursor, entry):
    '''
    Function to find the IMAGE03 s3 filepath of an IMAGE_AGGREGATE entry
    and add it to the entry tuple

    Parameters
    ----------
    cursor : OracleCursor
        a cx_Oracle cursor object which is used to query and modify an
        Oracle database
    entry : tuple
        a 10-element tuple containing:
        (subjectkey, interview_age, subject_id, image_category,
         image_dimensions, image_subtyp, image_scanner_manufacturer,
         image_tr, image_te, image_flip_angle)
        from the IMAGE_AGGREGATE table in the miNDAR DB instance 

    Returns
    -------
    new_list : tuple
        This method returns the input entry tuple with the s3 filepath
        (taken from IMAGE03) appended to the end
    '''
    
    # Init variables
    # Get all relevant data from IMAGE03 using IMAGE_AGGREGATE results
    img03_cmd = '''
                select subjectkey, interview_age, interview_date,
                src_subject_id, image_description, image_modality,
                scanner_manufacturer_pd, mri_repetition_time_pd,
                mri_echo_time_pd, flip_angle, scan_type, image_file,
                image03_id
                from 
                image03
                where
                subjectkey = :arg_1 and
                interview_age = :arg_2 and
                image_description = :arg_3
                '''
    # Get values from entry tuple
    subkey = entry[0]
    age = entry[1]
    img_type = entry[5]

    # Match where IMG_AGG image_subtype = IMG03 image_description
    cursor.execute(img03_cmd, arg_1=subkey, arg_2=age, arg_3=img_type)
    res = cursor.fetchall()

    # If we found multiple image types, filter through
    no_res = len(res)
    if no_res > 1:
        print 'Found %d results for %s %s entries in '\
              'IMAGE03, just taking the first one for now...'\
              % (no_res, subkey, img_type)
    # Take the first entry
    res = res[0]
    entry_list = list(entry)
    # Append the S3 file path to the end
    entry_list.append(res[-2])

    # Return it as a tuple
    return tuple(entry_list)


# Unzip and extract data from S3 via ndar_unpack
def run_ndar_unpack(s3_path, out_nii, aws_access_key_id, 
                                      aws_secret_access_key):
    '''
    Function to execute the ndar_unpack python script included in this
    package

    Parameters
    ----------
    s3_path : string
        full filepath to the s3 file location
        (e.g. s3://bucket/path/image.zip)
    out_nii : string
        filepath of the nifti file to write to disk
    aws_access_key_id : string
        string of the AWS access key ID
    aws_secret_access_key : string
        string of the AWS secret access key

    Returns
    -------
    None or Exception
        if the function successfully runs, it will return nothing;
        however, if there is an error in running the system command, the
        function will raise an exception
    '''

    # Import packages
    import os
    import subprocess

    # Init variables
    cmd_list = ['./ndar_unpack', '--aws-access-key-id', aws_access_key_id, 
                '--aws-secret-access-key', aws_secret_access_key, 
                '-v', out_nii, s3_path]

    # Run the command
    p = subprocess.Popen(cmd_list, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
    p.wait()
    stdout, stderr = p.communicate()

    # If the output doesn't exist, raise an OSError
    if not os.path.exists(out_nii):
        raise OSError('ndar_unpack failed, responded with:\n'\
                      'stdout: %s\n\nstderror: %s' % (stdout, stderr))

# Main routine
def main(inputs_dir, study_name, creds_path, sublist_yaml):
    '''
    Function generally used for task-specific scripting of functions
    declared in this module

    Parameters
    ----------
    inputs_dir : string
        filepath to the directory where all of the subjects' folders
        and sub-folders and niftis will be written to
    study_name : string
        the name of the study/site that all of the subjects will be
        placed in
    creds_path : string
        path to the csv file with 'Access Key Id' as the header and the
        corresponding ASCII text for the key underneath; same with the
        'Secret Access Key' string and ASCII text
    sublist_yaml : string
        filepath to output the subject list yaml file to

    Returns
    -------
    sublist : list (dict)
        Returns a list of dictionaries where the format of each dict-
        ionary is as follows:
        {'anat': '/path/to/anat.nii.gz',
         'rest': {rest_1_rest: '/path/to/rest_1.nii.gz',
                  rest_2_rest: '/path/to/rest_2.nii.gz',
                  ...}
         'subject_id': 'subject1234'
         'unique_id': 'session_1'}
    '''

    # Import packages
    import fetch_creds
    import os
    import sys
    import yaml
    import sys

    # Init variables
    aws_access_key_id, aws_secret_access_key = \
            fetch_creds.return_aws_keys(creds_path)
    cursor = fetch_creds.return_cursor(creds_path)

    # Take care of formatting/creating inputs directory
    inputs_dir = os.path.abspath(inputs_dir)
    if not inputs_dir.endswith('/'):
        inputs_dir = inputs_dir.rstrip('/')
    # Create the directory if it does not exist
    if not os.path.exists(inputs_dir):
        try:
            print 'creating inputs directory: %s' % inputs_dir
            os.makedirs(inputs_dir)
        except OSError as e:
            print 'Unable to make inputs directory %s' % inputs_dir
            print 'This might be due to permissions: %s' %e
            sys.exit()

    # Test the yaml subject list file for errors
    sublist_yaml = os.path.abspath(sublist_yaml)
    if os.path.exists(sublist_yaml):
        print '%s already exists, please specify a different path'
        sys.exit()
    elif os.access(os.path.dirname(sublist_yaml), os.W_OK):
        print 'sublist will be written to %s' % sublist_yaml
    else:
        print 'cannot write to output directory for sublist %s, please '\
              'specify a different path' % sublist_yaml

    # Get image aggregate results
    # IMAGE_AGGREGATE    --->       IMAGE03 columns          EXAMPLE
    # ---------------               ---------------          -------
    # subjectkey                    subjectkey               'NDARABCD1234'
    # image_subtype                 image_description        'EPI', 'MPRAGE'
    # image_category                image_modality           'MRI', 'FMRI'
    # image_scanner_manufacturer    scanner_manufacturer_pd  'SIEMENS'
    # image_tr                      mri_repetition_time_pd   '2.53'
    # image_te                      mri_echo_time_pd         '0.033'
    # image_flip_angle              flip_angle               '90'

    # Query commands
    # Get all of the data from IMAGE_AGGREGATE
    agg_cmd = '''
              select subjectkey, interview_age, subject_id,
              image_category, image_dimensions, image_subtype,
              image_scanner_manufacturer, image_tr, image_te, 
              image_flip_angle
              from
              image_aggregate
              '''
    
    # Get initial list form image_aggregate table
    cursor.execute(agg_cmd)
    agg_list = cursor.fetchall()

    # Build list of unique subjectkeys
    subkeys = [i[0] for i in agg_list]
    subkeys = list(set(subkeys))
    # Go through IMAGE_AGGREGATE; create anat/rest dictionary of subjectkeys
    subkey_dict = {i:{'anat' : [], 'rest' : []} for i in subkeys}
    for i in agg_list:
        subkey = i[0]
        img_type = i[3].lower()
        if 'fmri' in img_type or 'frmi' in img_type:
            subkey_dict[subkey]['rest'].append(i)
        elif 'mri' in img_type:
            subkey_dict[subkey]['anat'].append(i)
        else:
            print 'unkown image type for entry, skipping...'
            print i

    # Prune any subjects that don't have both MRI and fMRI data
    subkey_dict = {k:v for k,v in subkey_dict.items()\
                       if k and v['anat'] and v['rest']}
    # Iterate through dictionary to query IMAGE03 for S3 file paths
    for k,v in subkey_dict.items():
        # Get IMAGE_AGGREGATE entries for anat/rest
        anat_entries = v['anat']
        rest_entries = v['rest']
        print k
        # For each anatomical image in IMAGE_AGGREGATE
        for a in anat_entries:
            i = 0
            new_entry = add_s3_path(cursor, a)
            subkey_dict[k]['anat'][i] = new_entry
            i += 1
        # For each functional image in IMAGE_AGGREGATE
        i = 0
        for r in rest_entries:
            new_entry = add_s3_path(cursor, r)
            subkey_dict[k]['rest'][i] = new_entry
            i += 1

    # Now create cpac-sublist, unique id is interview age for now
    # Also restricted to 1 anatomical image for now
    sublist = [{'subject_id': str(k),
                'unique_id': v['anat'][0][1],
                'anat': v['anat'][0][-1], 
                'rest': {'rest_%d_rest' % (i+1) : 
                         v['rest'][i][-1] for i in range(len(v['rest']))}
                }
               for k,v in subkey_dict.items()]

    # Iterate through sublist to create filepaths and run ndar_unpack
    no_subs = len(sublist)
    for sub in sublist:
        idx = sublist.index(sub)
        # First create subject directories
        unique_sub_dir = '/'.join([inputs_dir,
                                   study_name,
                                   sub['subject_id'],
                                   sub['unique_id']])
        # If the file directory doesn't exist already
        if not os.path.exists(unique_sub_dir):
            print 'creating subject/session directories: %s' % unique_sub_dir
            os.makedirs(unique_sub_dir)

        # ndar_unpack the anatomical
        anat_dir = unique_sub_dir + '/anat_1'
        if not os.path.exists(anat_dir):
            print 'creating anatomical directory: %s' % anat_dir
            os.makedirs(anat_dir)
        # Set nifti file output
        s3_path = sub['anat']
        out_nii = anat_dir + '/' + 'anat.nii.gz'
        # And try and extract the image
        try:
            print 'attempting to download and extract %s to %s'\
                  % (s3_path, out_nii)
            run_ndar_unpack(s3_path, out_nii, aws_access_key_id, 
                                              aws_secret_access_key)
            print 'Success!'
            # If it is successful, replace s3_path with out_nii
            sublist[idx]['anat'] = out_nii
        except OSError as e:
            print e
            print 'Failed anatomical image %s extraction for %s.\n'\
                  'Trying functional...' % (s3_path, sub['subject_id'])

        # ndar_unpack the functional
        for folder, s3_path in sub['rest'].items():
            rest_dir = unique_sub_dir + '/' + folder.split('_rest')[0]
            if not os.path.exists(rest_dir):
                print 'creating functional directory: %s' % rest_dir
                os.makedirs(rest_dir)
            out_nii = rest_dir + '/' + 'rest.nii.gz'
            # And try and extract the image
            try:
                print 'attempting to download and extract %s to %s'\
                      % (s3_path, out_nii)
                run_ndar_unpack(s3_path, out_nii, aws_access_key_id, 
                                                  aws_secret_access_key)
                print 'Success!'
                # If it is successful, replace s3_path with out_nii
                sublist[idx]['rest'][folder] = out_nii
            except OSError as e:
                print e
                print 'Failed functional image %s extraction for %s'\
                      % (s3_path, sub['subject_id'])

        # Print % complete
        i = idx+1
        per = 100*(float(i)/no_subs)
        print 'Done extracting %d/%d\n%f%% complete' % (i, no_subs, per)


    # And write it to disk
    with open(sublist_yaml,'w') as f:
        f.write(yaml.dump(sublist))

    # Return the subject list
    return sublist

# Run main by default
if __name__ == '__main__':

    # Import packages
    import sys

    # Init variables
    try:
        inputs_dir = str(sys.argv[1])
        study_name = str(sys.argv[2])
        creds_path = str(sys.argv[3])
        sublist_yaml = str(sys.argv[4])
    except IndexError as e:
        print 'Not enough input arguments, got IndexError: %s' % e
        print __doc__
        sys.exit()

    # Run main
    sublist = main(inputs_dir, study_name, creds_path, sublist_yaml)
