# check_entries.py
#
# Author: Daniel Clark, 2014

'''
This module checks the miNDAR database tables for existing entries from
an input list of subject datasetids. If there is no entry or there are
partial entries, it deletes the partial entries and adds the new ones.

Usage:
    python check_entries.py -c <creds_path> -t <table_name> -i <ids_yaml>
                            [-b <bucket_name>] [-r <roi_map>]
'''

# Get next primary key id
def get_next_pk(cursor, table, pk_id):
    '''
    Method to return the next (highest+1) primary key from a table to
    use for the next entry. If no entries are found, the method will
    return 1.

    Parameters
    ----------
    cursor : OracleCursor
        a cx_Oracle cursor object which is used to query and modify an
        Oracle database
    table : string
        name of the table to query
    pk_id : string
        field name of the column that contains the primary keys

    Returns
    -------
    pk_id: integer
        the next primary key to use for that table
    '''

    # Init variables
    cmd = 'select max(%s) from %s' %(pk_id, table)

    # Query database and get results
    cursor.execute(cmd)
    res = cursor.fetchall()[0][0]

    # If it has a value, increment and return
    if res:
        pk_id = int(res) + 1
    # Otherwise, consider this the first entry, return 1
    else:
        pk_id = 1

    # Return the primary key
    return pk_id


# Get ROI txt file from S3 bucket and return as dict object
def get_roi_dict(creds_path, bucket_name, datasetid):
    '''
    Function to read txt stream from URL of an ROI file generated
    from an ANTs cortical thickness run

    Parameters
    ----------
    creds_path : string
        path to the csv file with 'Access Key Id' as the header and the
        corresponding ASCII text for the key underneath; same with the
        'Secret Access Key' string and ASCII text
    bucket : string
        the name of the bucket to get the ROI txt file from
    datasetid : string
        the dataset id of interest

    Returns
    -------
    sub_dict : dictionary {str : str}
        the ROI dictionary with the ROI label (key) mapped to its ROI
        value
    '''

    # Import packages
    import fetch_creds

    # Init variables
    bucket = fetch_creds.return_bucket(creds_path, bucket_name)
    key_path = 'outputs/' + datasetid + '/' + datasetid + '_ROIstats.txt'
    key = bucket.get_key(key_path)

    # Get file contents and split into list
    kstring = key.get_contents_as_string()
    temp_list = kstring.split('\n')

    # Form subject ROI dictionary
    key = temp_list[0].split()[2:]
    val = temp_list[1].split()[2:]
    sub_dict = dict(zip(key,val))

    # Return the subject ROI dictionary
    return sub_dict


# Function to load the ROIS to the unorm'd database
def insert_unormd(cursor, img03_id_str, table_name,
                  s3_path=None, roi_map=None, roi_dict=None):
    '''
    Method to return the next (highest+1) primary key from a table to
    use for the next entry. If no entries are found, the method will
    return 1.

    Parameters
    ----------
    cursor : OracleCursor
        a cx_Oracle cursor object which is used to query and modify an
        Oracle database
    img03_id_str : string
        string of the image03_id of the input subject to process
    table_name : string
        name of the table to insert entries in to
    s3_path : string
        S3 file path location on AWS
    roi_map : dictionary {str : str} (optional)
        a dictionary containing the mapping between the ROI label (key)
        and the ROI anatomical label (value) for a particular atlas
    roi_dict : dictionary {str : str} (optional)
        a dictionary of the subject's ROI labels and values; this
        parameter is only necessary when inserting ROI entries. If this
        is not set, the function will only insert a single entry

    Returns
    -------
    None
        The function doesn't return any value, it inserts an entry into
        the un-normalized database tables
    '''
    
    # Import packages
    import time

    # Init variables
    # Constant arguments for all entries
    atlas_name = 'OASIS-TRT-20_jointfusion_DKT31_CMA_labels_in_OASIS-30.nii.gz'
    atlas_ver = '2mm (2013)'
    pipeline_name = 'ndar_act_workflow.py'
    pipeline_type = 'nipype workflow'
    cfg_file_loc = 's3://ndar-data/scripts/ndar_act_workflow.py'
    pipeline_tools = 'ants, nipype, python'
    pipeline_ver = 'v0.2'
    # Get guid cmd
    get_guid_cmd = '''
                   select subjectkey from nitrc_image03 
                   where
                   image03_id = :arg_1
                   '''
    img03_id = int(img03_id_str)
    cursor.execute(get_guid_cmd, arg_1=img03_id)
    guid = cursor.fetchall()[0][0]
    # If roi dictionary is passed in, insert ROI means 
    if roi_dict:
        deriv_name = 'cortical thickness'
        # Get next deriv_id here
        deriv_id = get_next_pk(cursor, 'derivatives_unormd','id') 
        # Command string
        cmd = '''
              insert into %s
              (id, atlasname, atlasversion, roi, roidescription, 
               pipelinename, pipelinetype, cfgfilelocation, pipelinetools, 
               pipelineversion, pipelinedescription, derivativename, measurename, 
               datasetid, timestamp, value, units, guid)
              values
              (:col_1, :col_2, :col_3, :col_4, :col_5, :col_6, :col_7, :col_8, :col_9, 
               :col_10, :col_11, :col_12, :col_13, :col_14, :col_15, :col_16, 
               :col_17, :col_18)
              '''
        pipeline_desc = 'compute the mean thickness of cortex in ROI'
        measure_name = 'mean'
        units = 'mm'
        # Iterate through ROI dictionary to upload all ROI values
        for k,v in roi_dict.iteritems():
            # Timestamp
            timestamp = str(time.ctime(time.time()))
            # Get ROI number and name from dictionaries
            roi = k.split('Mean_')[1]
            roi_name = roi_map[k]
            # Get ROI value from dictionary
            value = float(v)
            # Execute insert command
            cursor.execute(cmd % 'derivatives_unormd',
                           col_1 = deriv_id,
                           col_2 = atlas_name,
                           col_3 = atlas_ver,
                           col_4 = roi,
                           col_5 = roi_name,
                           col_6 = pipeline_name,
                           col_7 = pipeline_type,
                           col_8 = cfg_file_loc,
                           col_9 = pipeline_tools,
                           col_10 = pipeline_ver,
                           col_11 = pipeline_desc,
                           col_12 = deriv_name,
                           col_13 = measure_name,
                           col_14 = img03_id,
                           col_15 = timestamp,
                           col_16 = value,
                           col_17 = units,
                           col_18 = guid)
            # ...and increment the primary key
            deriv_id +=1

    # Otherwise, inserting nifti file derivative
    elif s3_path:
        deriv_name = 'Normalized cortical thickness image'
        # Get next deriv_id here
        deriv_id = get_next_pk(cursor, 'img_derivatives_unormd','id') 
        cmd = '''
              insert into %s
              (id, roi, pipelinename, pipelinetype, cfgfilelocation, 
              pipelinetools, pipelineversion, pipelinedescription, name, 
              measurename, timestamp, s3_path, template, guid, datasetid, 
              roidescription)
              values
              (:col_1, :col_2, :col_3, :col_4, :col_5, :col_6, :col_7, :col_8,
               :col_9, :col_10, :col_11, :col_12, :col_13, :col_14, :col_15, :col_16)
              '''
        # Timestamp
        timestamp = str(time.ctime(time.time()))
        # Pipeline desc and measure fields
        pipeline_desc = 'compute the cortical thickness from anatomical image '\
                        'in subject space, and normalize to template'
        measure_name = 'image'
        roi = 'Grey matter'
        roi_desc = 'Grey matter cortex'
        template = 'OASIS-30_Atropos Template'
        # Execute insert command
        cursor.execute(cmd % 'img_derivatives_unormd',
                       col_1 = deriv_id,
                       col_2 = roi,
                       col_3 = pipeline_name,
                       col_4 = pipeline_type,
                       col_5 = cfg_file_loc,
                       col_6 = pipeline_tools,
                       col_7 = pipeline_ver,
                       col_8 = pipeline_desc,
                       col_9 = deriv_name,
                       col_10 = measure_name,
                       col_11 = timestamp,
                       col_12 = s3_path,
                       col_13 = template,
                       col_14 = guid,
                       col_15 = img03_id,
                       col_16 = roi_desc)
        # ...and increment the primary key
        deriv_id +=1
    # and commit changes
    cursor.execute('commit')

# Main routine
def main(creds_path, table_name, ids_yml, bucket_name=None, roi_map_yml=None):
    '''
    Function to query the table of interest for entries in the datasetid list
    from the ids_yaml file.

    Parameters
    ----------
    creds_path : string
        path to the csv file with 'Access Key Id' as the header and the
        corresponding ASCII text for the key underneath; same with the
        'Secret Access Key' string and ASCII text
    table_name : string
        the name of the table to query in miNDAR database
    ids_yml : string
        filepath to the input yaml file that contains a list of
        datasetids to query
    bucket_name : string (optional)
        the name of the bucket to get data from; only needed for ROI
        entries upload
    roi_map_yml : string (optional)
        filepath to the input yaml file that contains a dictionary of
        roi labels and names; only needed for ROI entries upload

    Returns
    -------
    None
        This function does not return a value.
    '''

    # Import packages
    import fetch_creds
    import sys
    import yaml

    # Init variables
    cursor = fetch_creds.return_cursor(creds_path)
    ids_list = yaml.load(open(ids_yml,'r'))
    no_files = len(ids_list)
    s3_prefix = 's3://ndar_data/outputs/'
    # Init roi mapping dictionary if it was specified
    if roi_map_yml:
        roi_map_dict = yaml.load(open(roi_map_yml, 'r'))
        num_entries = len(roi_map_dict)
    else:
        roi_map_dict = None
        num_entries = 1

    i = 0
    # Go through the list
    for id in ids_list:
        cmd = 'select * from %s where datasetid = :arg_1' % table_name
        cursor.execute(cmd, arg_1=id)
        res = cursor.fetchall()
        num_res = len(res)
        # If the number of entries isn't what we expect
        if num_res < num_entries:
            # If there is an incomplete number of entries, delete them
            if num_res > 0:
                print 'Deleting partially-populated entries with datasetid = %s' % id
                cursor.execute('delete from %s where datasetid = :arg_1', arg_1=id)
            # If we're loading in ROIs, get the roi_dic from the S3 bucket
            if roi_map_dict:
                roi_dict = get_roi_dict(creds_path, bucket_name, id)
                s3_path = None
            else:
                roi_dict = None
                s3_path = s3_prefix + id + '/' + id + \
                          '_corticalthickness_normd.nii.gz'
            # And populate the table entries
            insert_unormd(cursor, id, table_name, s3_path=s3_path,
                          roi_map=roi_map_dict, roi_dict=roi_dict)
            print 'Successfully inserted entry %s!' % id
        # If we see more than we expect, raise an error
        elif num_res > num_entries:
            raise ValueError, 'more entries found than expected, investigate '\
                              'this manually, datasetid: %s' % id
            sys.exit()
        # Otherwise, the amount of entries is the amount we expect, move on
        else:
            print 'Found the right amount of entries, dataset: %s is good' % id
        # Increment counter
        i += 1
        per = 100*(float(i)/no_files)
        print 'done with file %d/%d\n%f%% complete\n' % \
        (i, no_files, per)


# Run main by default
if __name__ == '__main__':

    # Import packages
    import argparse
    import os
    import sys

    # Init argparser
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-c', '--creds', nargs=1, required=True,
                        help='Filepath to the credentials file')
    parser.add_argument('-t', '--table', nargs=1, required=True,
                        help='miNDAR database table name to insert entries')
    parser.add_argument('-i', '--input', nargs=1, required=True,
                        help='Filepath to input file list of dataset ids')
    parser.add_argument('-b', '--bucket', nargs=1, required=False,
                        help='Name of the S3 bucket to get ROI text file')
    parser.add_argument('-r', '--roi_map', nargs=1, required=False,
                        help='Filepath to local roi map yaml file')
    args = parser.parse_args()

    # Init variables
    # Required arguments
    creds_path = os.path.abspath(args.creds[0])
    table_name = str(args.table[0])
    ids_yaml = os.path.abspath(args.input[0])
    # Optional arguments
    if args.bucket:
        bucket_name = str(args.bucket[0])
    else:
        bucket_name = None
    if args.roi_map:
        roi_map = os.path.abspath(args.roi_map[0])
    else:
        roi_map = None

    # Run main
    main(creds_path, table_name, ids_yaml,
         bucket_name=bucket_name, roi_map_yml=roi_map)
