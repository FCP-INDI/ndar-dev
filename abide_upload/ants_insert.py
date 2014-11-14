# ants_insert.py
#
# Author: Daniel Clark, 2014

'''
This module contains functions which populate the abide_img_results
table on the miNDAR database using ABIDE preprocessed data stored in
Amazon's S3 service.
'''

# Function to insert derivatives into the un-normalized database
def insert_unormd(roi_txt_fpaths, creds_path, oasis_file):
    '''
    Function to insert image results data for ANTs cortical thickness
    to the DERIVATIVES_UNORMD table in miNDAR.

    Parameters
    ----------
    roi_txt_fpaths : list (str)
        a list of filepaths as strings to the ROIstats.txt files to
        upload
    creds_path : string (filepath)
        path to the csv file with 'Access Key Id' as the header and the
        corresponding ASCII text for the key underneath; same with the
        'Secret Access Key' string and ASCII text
    oasis_file : string
        filepath to the Oasis_ROIs.txt file

    Returns
    -------
    None
        This function has no return value. It uploads the data from the
        list to a miNDAR database and exits.
    '''

    # Import packages
    import cx_Oracle
    import datetime
    import pytools
    import os

    # Init variables
    big_dic = {}

    # For each subject
    for sub in roi_txt_fpaths:
        temp_list = []
        # Gather each subjects ROIs
        with open(sub,'r') as f:
            for i,line in enumerate(f):
                temp_list.append(line.split())
        # Trim off top elements (not ROIs)
        key = temp_list[0][2:]
        val = temp_list[1][2:]
        big_dic[os.path.basename(sub)] = dict(zip(key,val))
        # Close ROI txt file
        f.close()

    # Build mapping dictionary
    roi_dic = {}
    with open(oasis_file) as f:
        for i,line in enumerate(f):
            # Split the line into list (tab delimiter)
            split_line = line.split('\t')
            # Filter out any blank strings in the list
            split_line = filter(None, split_line)
            # Filter out leading/trailing spaces
            key = split_line[0].strip()
            val = split_line[1].strip()
            # Store in dictionary
            roi_dic[key] = val

    # User and database info
    cursor = pytools.fetch_creds.return_cursor(creds_path)

    # Constant arguments for all entries
    atlas_name = 'OASIS-TRT-20_jointfusion_DKT31_CMA_labels_in_OASIS-30.nii.gz'
    atlas_ver = '2mm (2013)'
    pipeline_name = 'act_workflow.py'
    pipeline_type = 'nipype workflow'
    cfg_file_loc = '/path/to/act_workflow.py'
    pipeline_tools = 'ants, nipype, python'
    pipeline_ver = 'v0.1'
    pipeline_desc = 'compute the mean thickness of cortex in ROI for the ABIDE dataset'
    deriv_name = 'cortical thickness'
    measure_name = 'mean'
    units = 'mm'
    # Get the next derivativeid (primary key from table)
    deriv_id = return_next_pk(cursor, 'derivatives_unormd')

    # Command string
    cmd = '''
          insert into derivatives_unormd
          (id, atlasname, atlasversion, roi, roidescription, pipelinename, pipelinetype, 
          cfgfilelocation, pipelinetools, pipelineversion, pipelinedescription, 
          derivativename, measurename, datasetid, timestamp, value, units, guid)
          values
          (:col_1, :col_2, :col_3, :col_4, :col_5, :col_6, :col_7, :col_8, :col_9, 
           :col_10, :col_11, :col_12, :col_13, :col_14, :col_15, :col_16, :col_17,
           :col_18)
          '''

    # Iterate through dictionary and upload data
    not_in_nitrc = []
    for key, val in big_dic.iteritems():
        # Find subject in image03 to get datasetID
        dataset_id = key.split('_')[0]
        print dataset_id
        id_find = '''
                  select guid
                  from abide_subjects
                  where id = :arg_1
                  '''
        cursor.execute(id_find, arg_1=dataset_id)
        res = cursor.fetchall()
        guid = res[0][0]
        print 'dataset_id ', dataset_id
        print 'guid', guid
        # Iterate through ROIs
        for k, v in val.iteritems():
            # Timestamp
            timestamp = str(datetime.datetime.now())
            # Get ROI number
            roi = k.split('Mean_')[1]
            roi_name = roi_dic[k]
            # Value
            value = float(v)
            # Execute insert command
            cursor.execute(cmd,
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
                           col_14 = dataset_id,
                           col_15 = timestamp,
                           col_16 = value,
                           col_17 = units,
                           col_18 = guid)
            # Increment the unique id
            deriv_id += 1
            print 'deriv_id ', deriv_id

    # And commit changes
    cursor.execute('commit')


# Function to insert image derivatives into the un-normalized database
def insert_img_unormd(id_s3_list, creds_path):
    '''
    Function to insert image results data for ANTs cortical thickness
    to the IMG_DERIVATIVES_UNORMD table in miNDAR.

    Parameters
    ----------
    id_s3_list : list (tuple)
        a list of tuples where each tuple contains 2 strings:
        (datasetid, s3_path)
    creds_path : string (filepath)
        path to the csv file with 'Access Key Id' as the header and the
        corresponding ASCII text for the key underneath; same with the
        'Secret Access Key' string and ASCII text

    Returns
    -------
    None
        This function has no return value. It uploads the data from the
        list to a miNDAR database and exits.
    '''

    # Import packages
    import cx_Oracle
    import datetime
    import pytools
    import os
    import yaml

    # Init variables
    # Create cursor for queries and data inserts
    cursor = pytools.fetch_creds.return_cursor(creds_path)

    # Constant arguments for all entries
    pipeline_name = 'act_workflow.py'
    deriv_name = 'Normalized cortical thickness image'
    measure_name = 'image'

    # Knowns
    roi_id = 'Grey matter'
    roi_description = 'Grey matter cortex'
    template = 'OASIS-30_Atropos Template'
    atlas_name = 'OASIS-TRT-20_jointfusion_DKT31_CMA_labels_in_OASIS-30.nii.gz'
    atlas_ver = '2mm (2013)'
    pipeline_name = 'act_workflow.py'
    pipeline_type = 'nipype workflow'
    cfg_file_loc = '/path/to/act_workflow.py'
    pipeline_tools = 'ants, nipype, python'
    pipeline_ver = 'v0.1'
    pipeline_desc = 'compute the cortical thickness of an extracted brain in ' \
                    'subject space, and normalize to template'

    # Get the next derivativeid (primary key from table)
    deriv_id = return_next_pk(cursor, 'img_derivatives_unormd')

    # Command string
    cmd = '''
          insert into img_derivatives_unormd
          (id, roi, pipelinename, pipelinetype, cfgfilelocation, 
          pipelinetools, pipelineversion, pipelinedescription, name, measurename, 
          timestamp, s3_path, template, guid, datasetid, roidescription)
          values
          (:col_1, :col_2, :col_3, :col_4, :col_5, :col_6, :col_7, :col_8,
           :col_9, :col_10, :col_11, :col_12, :col_13, :col_14, :col_15, :col_16)
          '''

    # Iterate through dictionary and upload data
    for sub in id_s3_list:
        # Timestamp
        timestamp = str(datetime.datetime.now())
        # Get datasetid and s3_path
        dataset_id = sub[0]
        s3_path = sub[1]
        # Get id
        id_find = '''
                  select guid from abide_subjects
                  where id = :arg_1
                  '''
        cursor.execute(id_find, arg_1=dataset_id)
        res = cursor.fetchall()
        guid = res[0][0]
        # Execute insert command
        cursor.execute(cmd,
                       col_1 = int(deriv_id),
                       col_2 = roi_id,
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
                       col_15 = dataset_id,
                       col_16 = roi_description)
        # Increment the unique id
        print 'deriv_id ', deriv_id
        deriv_id += 1

    # Commit the changes and close the cursor/connection
    cursor.execute('commit')
    cursor.close()


# Read in ROI txt file and insert
def read_roi_from_url(url_path):
    '''
    Function to read txt stream from URL of an ROI file generated
    from an ANTs cortical thickness run

    Parameters
    ----------
    url_path : string
        URL address of the text file to read in

    Returns
    -------
    None
        This function does not return any value. It uploads the
        contents of the ROI txt file to the miNDAR database table
        ABIDE_IMAGE_RESULTS
    '''

    # Import packages
    import urllib

    # Init variables
    temp_list = []

    # Store file contents as list of strings
    url_file = urllib.urlopen(url_path)
    url_list = url_file.readlines()

    # Split lines and store temporarily
    for line in url_list:
        temp_list.append(line.split())

    # Form subject ROI dictionary
    sub_dict = {}
    key = temp_list[0][2:]
    val = temp_list[1][2:]
    sub_id = 'test'
    sub_dict[sub_id] = dict(zip(key,val))

    return sub_dict


# Make ROI S3 path from ACT S3 path
def make_roi_s3(cursor, datasetid):
    '''
    Function to make the corresponding ROI txt thickness S3 path from
    a ACT image S3 path
    '''

    # Init variables
    s3_get = '''
             select s3_path
             from
             img_derivatives_unormd
             where
             datasetid = :arg_1
             '''

    # Get the ACT S3 path
    cursor.execute(s3_get, arg_1=datasetid)
    s3_path = cursor.fetchone()[0]

    # Split the path and substitute filename
    s3_list = s3_path.split('/')
    s3_list[-2] = 'roi_thickness'
    fn = s3_list[-1]
    fn_new = fn.replace('_anat_thickness.nii.gz','_roi_thickness.txt')
    s3_list[-1] = fn_new

    # Create new S3 path
    s3_path = '/'.join(s3_list)

    # And return the new path
    return s3_path


# Transfer results between tables
def transfer_table_entries(creds_path):
    '''
    Function to transfer all of the ABIDE subjects results in the
    DERIVATIVES_UNORMD and IMG_DERIVATIVES_UNORMD tables to the 
    ABIDE_IMG_RESULTS table

    Parameters
    ----------
    creds_path : string (filepath)
        path to the csv file with 'Access Key Id' as the header and the
        corresponding ASCII text for the key underneath; same with the
        'Secret Access Key' string and ASCII text

    Returns
    -------
    None
        This function does not return any value. It transfers table
        entries in an Oracle database.
    '''

    # Import packages
    import insert_utils
    import pytools

    # Init variables
    deriv_id = insert_utils.return_next_pk(cursor, 'ABIDE_IMG_RESULTS')
    template = 'OASIS-30 Atropos Template'
    cursor = pytools.fetch_creds.return_cursor(creds_path)
    # Get ACT img derivatives from img_derivatives_unormd
    imgs_get = '''
               select
               pipelinename, pipelinetype, pipelinetools, pipelineversion,
               pipelinedescription, name, measurename, guid,
               datasetid, roidescription, roi, template, s3_path, cfgfilelocation
               from
               img_derivatives_unormd
               where instr(datasetid, :arg_1) > 0
               '''
    # Get ROI derivatives from DERIVATIVES_UNORMD
    rois_get = '''
               select
               pipelinename, pipelinetype, pipelinetools, pipelineversion, 
               pipelinedescription, derivativename, measurename, guid, 
               datasetid, roidescription, roi, template, value, units,
               cfgfilelocation
               from
               derivatives_unormd
               where instr(datasetid, :arg_1) > 0
               '''
    # Insert entries into ABIDE_IMG_RESULTS
    air_put = '''
              insert into abide_img_results
              (id, pipelinename, pipelinetype, pipelinetools,
              pipelineversion, pipelinedescription, name, measurename, 
              timestamp, guid, datasetid, roidescription, roi, atlas, value,
              units, s3_path, template, cfgfilelocation)
              values
              (:col_1, :col_2, :col_3, :col_4, :col_5, :col_6, :col_7, :col_8,
              :col_9, :col_10, :col_11, :col_12, :col_13, :col_14, :col_15,
              :col_16, :col_17, :col_18, :col_19)
              '''

    # Get abide results from derivatives_unormd (ABIDE id's have an 'a' in them)
    cursor.execute(rois_get, arg_1='a')
    roi_entries = cursor.fetchall()

    print 'Found %d roi results, inserting into ABIDE table' % len(roi_entries)
    # For each ROI entry, copy its fields over to ABIDE_IMG_RESULTS
    for entry in roi_entries:
        # Extract field values from entry result
        pname = entry[0]
        ptype = entry[1]
        ptools = entry[2]
        pver = entry[3]
        pdesc = entry[4]
        dname = entry[5]
        mname = entry[6]
        guid = entry[7]
        datasetid = entry[8]
        roidesc = entry[9]
        roi = entry[10]
        # template --> atlas
        atlas = entry[11]
        value = entry[12]
        units = entry[13]
        cfgfile = entry[14]
        # Timestamp
        timestamp = str(time.ctime(time.time()))
        # Find/make s3 path
        s3_path = make_roi_s3(cursor, datasetid)
        # And insert all of this into ABIDE_IMG_RESULTS
        cursor.execute(air_put, col_1=deriv_id,
                                col_2=pname,
                                col_3=ptype,
                                col_4=ptools,
                                col_5=pver,
                                col_6=pdesc,
                                col_7=dname,
                                col_8=mname,
                                col_9=timestamp,
                                col_10=guid,
                                col_11=datasetid,
                                col_12=roidescription,
                                col_13=roi,
                                col_14=atlas,
                                col_15=value,
                                col_16=units,
                                col_17=s3_path,
                                col_18=template,
                                col_19=cfgfile)
        # Commit changes
        cursor.execute('commit')
        # Increment to next unique pk id
        deriv_id += 1
        print deriv_id

    # Get abide results from derivatives_unormd (ABIDE id's have an 'a' in them)
    cursor.execute(imgs_get, arg_1='a')
    img_entries = cursor.fetchall()
    print 'Found %d image results, inserting into ABIDE table' % len(img_entries)
    # For each IMG entry, copy its fields over to ABIDE_IMG_RESULTS
    for entry in img_entries:
        # Extract field values from entry result
        pname = entry[0]
        ptype = entry[1]
        ptools = entry[2]
        pver = entry[3]
        pdesc = entry[4]
        dname = entry[5]
        mname = entry[6]
        guid = entry[7]
        datasetid = entry[8]
        roidesc = entry[9]
        roi = entry[10]
        # template --> atlas
        template = entry[11]
        s3_path = entry[12]
        cfgfile = entry[13]
        # Timestamp
        timestamp = str(time.ctime(time.time()))
        # Find/make s3 path
        s3_path = make_roi_s3(cursor, datasetid)
        # And insert all of this into ABIDE_IMG_RESULTS
        cursor.execute(air_put, col_1=deriv_id,
                                col_2=pname,
                                col_3=ptype,
                                col_4=ptools,
                                col_5=pver,
                                col_6=pdesc,
                                col_7=dname,
                                col_8=mname,
                                col_9=timestamp,
                                col_10=guid,
                                col_11=datasetid,
                                col_12=roidescription,
                                col_13=roi,
                                col_14='',
                                col_15='',
                                col_16='',
                                col_17=s3_path,
                                col_18=template,
                                col_19=cfgfile)
        # Commit changes
        cursor.execute('commit')
        # Increment to next unique pk id
        deriv_id += 1
        print deriv_id
