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
                lower(image_description) = :arg_3
                '''
    # Get values from entry tuple
    subkey = entry[0]
    age = entry[1]
    img_type = entry[5].lower()

    # Match where IMG_AGG image_subtype = IMG03 image_description
    print 'querying for %s, %s, %s...' % (subkey, age, img_type)
    cursor.execute(img03_cmd, arg_1=subkey, arg_2=age, arg_3=img_type)
    res = cursor.fetchall()

    # If we found multiple image types, filter through
    no_res = len(res)
    if no_res > 1:
        print 'Found %d results for %s %s entries in '\
              'IMAGE03, just taking the first one for now...'\
              % (no_res, subkey, img_type)
    elif no_res == 0:
        raise Exception('unable to find any s3 data')
    # Take the first entry
    res = res[0]
    entry_list = list(entry)
    # Append the S3 file path to the end
    entry_list.append(res[-2])

    # Return it as a tuple
    return tuple(entry_list)


# Return an organized dictionary from the IMAGE_AGGREGATE table
def build_subkey_dict(cursor, agg_results):
    '''
    Function to take a list of entry results from querying the
    IMAGE_AGGREGATE table and return an organized dictionary of
    anatomical and functional data with S3 paths; organized by
    subjectkey

    Parameters
    ----------
    cursor : OracleCursor
        a cx_Oracle cursor object which is used to query and modify an
        Oracle database
    agg_results : list
        a list of tuples that correspond to the column values for each
        database entry

    Returns
    -------
    subkey_dict : dictionary
        a dictionary where the keys correspond to the subject GUIDs and
        the values are dictionaries comprising of the anatomical and
        functional entries for that subject GUID;
        example:
        {SUBJECTKEY : {'anat' : [(SUBJECTKEY, AGE, SUBJECTID,
                                  IMG_CATEGORY, IMG_DIM, IMG_SUBTYPE,
                                  SCANNER_MFG, TR, TE, FLIP_ANGLE, S3_PATH)],
                       'rest' : [(SUBJECTKEY, AGE, SUBJECTID,
                                  IMG_CATEGORY, IMG_DIM, IMG_SUBTYPE,
                                  SCANNER_MFG, TR, TE, FLIP_ANGLE, S3_PATH)]
                      }, ...
        ...}

    '''

    # Build list of unique subjectkeys
    subkeys = [agg_entry[0] for agg_entry in agg_results]
    subkeys = list(set(subkeys))

    # Go through IMAGE_AGGREGATE; create anat/rest dictionary of subjectkeys
    subkey_dict = {subkey : {'anat' : [], 'rest' : []} for subkey in subkeys}
    for agg_entry in agg_results:
        # Get subject GUID and image type (MRI or fMRI)
        subkey = agg_entry[0]
        img_type = agg_entry[5]
        # If unknown, set img_type to blank string
        if img_type == None:
            img_type = ''
        else:
            img_type = img_type.lower()
        # Check if img_type is MRI/fMRI
        if ('fmri' in img_type or 'resting' in img_type or 'epi' in img_type):
            subkey_dict[subkey]['rest'].append(agg_entry)
        elif ('mri' in img_type or 'structural' in img_type or 'mprage' in img_type):
            subkey_dict[subkey]['anat'].append(agg_entry)
        else:
            print 'unkown image type for entry, skipping...'
            print agg_entry

    # Prune any subjects that don't have both MRI and fMRI data
    subkey_dict = {subkey : entry_dict for subkey, entry_dict in \
                   subkey_dict.items() if entry_dict['anat'] and entry_dict['rest']}
    print 'found %d items with both anatomical and functional data' \
            % (len(subkey_dict))

    # Iterate through dictionary to query IMAGE03 for S3 file paths
    for subkey, entry_dict in subkey_dict.items():
        # Get IMAGE_AGGREGATE entries for anat/rest
        anat_entries = entry_dict['anat']
        rest_entries = entry_dict['rest']
        # For each anatomical image in IMAGE_AGGREGATE
        for a_entry in anat_entries:
            entries_tmp = []
            try:
                new_entry = add_s3_path(cursor, a_entry)
                entries_tmp.append(new_entry)
            except Exception as exc:
                print exc.message
                print 'Unable to find entry for', a_entry
        subkey_dict[subkey]['anat'] = entries_tmp
        # For each functional image in IMAGE_AGGREGATE
        for r_entry in rest_entries:
            entries_tmp = []
            try:
                new_entry = add_s3_path(cursor, r_entry)
                entries_tmp.append(new_entry)
            except Exception as exc:
                print exc.message
                print 'Unable to add entry for', r_entry
        subkey_dict[subkey]['rest'] = entries_tmp

    # Prune any subjects that don't have both MRI and fMRI data
    subkey_dict = {subkey : entry_dict for subkey, entry_dict in \
                   subkey_dict.items() if entry_dict['anat'] and entry_dict['rest']}
    print 'found %d items with both anatomical and functional data' \
            % (len(subkey_dict))

    # Return the subkey dictionary
    return subkey_dict


# Query and build phenotype file
def build_phenotype(cursor, subkey_dict):
    '''
    Function which takes thesubject key dictionary and builds a C-PAC-
    compatible phenotype file for use in C-PAC's group analysis

    Parameters
    ----------
    cursor : OracleCursor
        a cx_Oracle cursor object which is used to query and modify an
        Oracle database
    subkey_dict : dictionary
        a dictionary where the keys correspond to the subject GUIDs and
        the values are dictionaries comprising of the anatomical and
        functional entries for that subject GUID;

    Returns
    -------
    pheno_list
    '''

    # Import packages

    # Init variables




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
    sublist : list
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

    # Init variables
    aws_access_key_id, aws_secret_access_key = \
            fetch_creds.return_aws_keys(creds_path)
    cursor = fetch_creds.return_cursor(creds_path)

    # Create the directory if it does not exist
    if not os.path.exists(inputs_dir):
        try:
            print 'creating inputs directory: %s' % inputs_dir
            os.makedirs(inputs_dir)
        except OSError as err:
            print 'Unable to make inputs directory %s' % inputs_dir
            print 'This might be due to permissions: %s' % err
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

    # Query IMAGE_AGGREGATE for subject image info, get S3 path from IMAGE03
    # Here's how the column names correspond between the two:
    # IMAGE_AGGREGATE    --->       IMAGE03 columns          EXAMPLE
    # ---------------               ---------------          -------
    # subjectkey                    subjectkey               'NDARABCD1234'
    # image_subtype                 image_description        'MPRAGE', 'EPI'
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
    img_agg_results = cursor.fetchall()

    # Build subkey dictionary from query results
    subkey_dict = build_subkey_dict(img_agg_results, cursor)

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
        unique_sub_dir = os.path.join(inputs_dir, study_name,
                                      str(sub['subject_id']),
                                      str(sub['unique_id']))
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
        if not os.path.exists(out_nii):
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
        else:
            'Anatomical file %s exists! Skipping...' % out_nii

        # ndar_unpack the functional for each functional
        for folder, s3_path in sub['rest'].items():
            rest_dir = unique_sub_dir + '/' + folder.split('_rest')[0]
            if not os.path.exists(rest_dir):
                print 'creating functional directory: %s' % rest_dir
                os.makedirs(rest_dir)
            out_nii = rest_dir + '/' + 'rest.nii.gz'
            # And try and extract the image
            if not os.path.exists(out_nii):
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
            else:
                'Functional file %s eists! Skippng...' % out_nii

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
