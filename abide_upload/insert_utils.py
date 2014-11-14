# insert_utils.py
#
# Author: Daniel Clark, 2014

'''
This module contains functions which populate the abide_img_results
table on the miNDAR database using ABIDE preprocessed data stored in
Amazon's S3 service.
'''


# Check if url path was already uploaded
def check_existing(cursor, url_path, table_name, entries=1):
    '''
    Method to check for existing entries in an Oracle database instance
    table. If the number of entries that are found is less than entries
    (unless entries = 1), it deletes the found entries and returns as
    False.

    Parameters
    ----------
    cursor : OracleCursor
        a cx_Oracle cursor object which is used to query and modify an
        Oracle database
    url_path : string (url)
        URL address of the file to parse and insert into miNDAR
    table_name : string
        name of the Oracle table to search
    entries : integer
        number of entries to look for; found results will be determined
        incomplete (and return False) if less than this value

    Returns
    -------
    exists : boolean
        Returns True if entries are found and complete, False otherwise
    '''

    # Init variables
    exist_cmd = 'select id from %s where s3_path = :arg_1'
    del_cmd = 'delete from %s where s3_path = :arg_1'

    # Execute command
    cursor.execute(exist_cmd % table_name, arg_1=url_path)
    res = cursor.fetchall()

    # If not enough entries were found, delete them as incomplete
    if len(res) < entries:
        if len(res) > 0:
            print 'Found partially populated entries for %s.' % url_path
            print res
            print 'Deleting...'
            cursor.execute(del_cmd % table_name, arg_1=url_path)
        else:
            print 'No entries found for %s' % url_path
        exists = False
        return exists
    # Else, if found number of expected entries, return as exists
    elif len(res) == entries:
        print 'All entries for %s were found, skipping...' % url_path
        exists = True
        return exists
    else:
        err = 'Found more than %d entries. There might be duplicates.\n \
               Parse through the table and fix manually.'
        raise RuntimeError, err


# Delete duplicates
def delete_dups(creds_path):

    # Find the old items
    def find_old_items(in_list):
        out_list = []
        for l in in_list:
            if l[1].startswith('/path/'):
                out_list.append(int(l[0]))
        return out_list

    import fetch_creds
    cursor = fetch_creds.return_cursor(creds_path)

    cmd = '''
          select datasetid, count(datasetid)
          from derivatives_unormd
          group by datasetid
          having count (datasetid) > 97
          '''

    cursor.execute(cmd)
    dups = cursor.fetchall()
    dups = [d[0] for d in dups]

    find_cmd = 'select id,cfgfilelocation from derivatives_unormd where datasetid = :arg_1'
    del_cmd = 'delete from derivatives_unormd where id = :arg_1'


    i = 1
    for d in dups:
        cursor.execute(find_cmd, arg_1=d)
        found_list = cursor.fetchall()
        old_items = find_old_items(found_list)
        for oi in old_items:
            print 'deleting entry with id = %d' % oi
            cursor.execute(del_cmd, arg_1=oi)
            cursor.execute('commit')
        print 'done with %d/%d' % (i, len(dups))
        i += 1


# Find subject id from filename
def find_subid(filename):
    '''
    Method to extract the subject id from a filename
    '''

    # Init variables
    i = 0
    j = 0

    # Split by '_' delimiter and parse
    fsplit = filename.split('_')
    # Parse through and set j to the index of where the subid # is
    for fs in fsplit:
        if fs.isdigit():
            j = i
        i += 1

    # If j was set, then we found sub_id
    if j:
        # Strip away leading 0's
        sub_id = fsplit[j].lstrip('0')
        return sub_id
    # Else it wasn't in filename, just 
    else:
        err = 'No numeric subject ID found in %s' % filename
        raise ValueError, err


# Return the dataset ID and GUID
def return_datasetid_guid(cursor, subid):
    # Init variables
    ids_cmd = '''
              select id, guid from abide_subjects
              where
              sub_id = :arg_1
              '''
    # Query the database and get the result
    cursor.execute(ids_cmd, arg_1=subid)
    res = cursor.fetchall()[0]
    # Try to get the result
    try:
        datasetid = res[0]
        guid = res[1]
    except IndexError as e:
        print 'Could not find results for subject\n', e

    # Return datasetid and guid
    return datasetid, guid


# Return the new unique id for table entry
def return_next_pk(cursor, table_name):
    '''
    Function to grab the next unique primary key from a table

    Parameters
    ----------
    cursor : OracleCursor
        a cx_Oracle cursor object which is used to query and modify an
        Oracle database
    table_name : string
        name of the Oracle table to search

    Returns
    -------
    deriv_id : integer
        the next primary key to use from table table_name
    '''

    # Get the next derivativeid (primary key from table)
    pk_cmd = 'select max(id) from %s' % table_name
    cursor.execute(pk_cmd)
    res = cursor.fetchall()[0][0]
    if res:
        deriv_id = res + 1
    else:
        deriv_id = 1

    # Return the primary key
    return deriv_id 


# Insert ABIDE subjects into table
def insert_abide_subjects(creds_path, xls_pheno_guid_path):
    # Import packages
    import fetch_creds
    import pandas

    # Init variables
    xlsx_path = xls_pheno_guid_path

    # Connect to database with cursor
    cursor = fetch_creds.return_cursor(creds_path)

    # Load data
    guid_df = pandas.read_excel(xlsx_path)

    # For each subject in the xlsx file, upload their data to table
    nrows = guid_df.shape[0]
    for i in range(nrows):
        sub = guid_df.ix[i,:]
        guid = sub['GUID']
        # Test if it's a NaN (registers as a float)
        if type(guid) != float:
            guid = str(guid)
            site_id = str(sub['SITE_ID'])
            sub_id = str(int(sub['SUB_ID']))
            dx_group = sub['DX_GROUP']
            dsm_iv_tr = sub['DSM_IV_TR']
            age = sub['AGE_AT_SCAN']
            sex = sub['SEX']
            if sex == 1:
                sex = 'M'
            else:
                sex = 'F'
            handedness = str(sub['HANDEDNESS_CATEGORY'])
            # Command to insert record
            cmd = '''
                  insert into abide_subjects
                  (id, guid, site_id, sub_id, dx_group, dsm_iv_tr, age_at_scan,
                   sex, handedness)
                  values
                  (:col_1, :col_2, :col_3, :col_4, :col_5, :col_6, :col_7, 
                   :col_8, :col_9)
                  '''
            cursor.execute(cmd,
                           col_1 = i-1,
                           col_2 = guid,
                           col_3 = site_id,
                           col_4 = sub_id,
                           col_5 = dx_group,
                           col_6 = dsm_iv_tr,
                           col_7 = age,
                           col_8 = sex,
                           col_9 = handedness)
            # Print to screen
            print i, guid


# Insert ABIDE ROIs 1D files into table
def upload_results(cursor, url_path):
    # Import packages
    import fetch_creds
    import time

    # Init variables
    # Cursor command for inserting data
    cmd = '''
          insert into abide_img_results
          (id, roi, pipelinename, pipelinetype,  pipelinetools, pipelineversion, 
          pipelinedescription, name, measurename, timestamp, s3_path, template, 
          guid, datasetid, roidescription, strategy, atlas)
          values
          (:col_1, :col_2, :col_3, :col_4, :col_5, :col_6, :col_7, :col_8, :col_9, 
          :col_10, :col_11, :col_12, :col_13, :col_14, :col_15, :col_16, :col_17)
          '''

    # Dictionary of full atlas names
    atlas_dict = {'aal' : ['Automated Anatomical Labelling', 116],
                  'cc200' : ['Craddock 200', 200],
                  'cc400' : ['Craddock 400', 392],
                  'dosenbach160' : ['Dosenbach 160', 161],
                  'ez' : ['Eickhoff-Zilles', 116],
                  'ho' : ['Harvard-Oxford', 111],
                  'tt' : ['Talaraich and Tournoux', 97]}

    # Dictionary of pipeline information (name: [desc, tools, type, ver])
    pipeline_dict = {'ccs' : ['Connectome Computation System', 
                              'AFNI, FSL, Freesurfer', 
                              'bash',
                              'v1.0beta'],
                     'cpac' : ['Configurable Pipeline for the Analysis of Connectomes',
                               'ANTs, AFNI, FSL, Nipype',
                               'Python',
                               'v0.3.4'],
                     'dparsf' : ['Data Processing Assistant for Resting-State fMRI', 
                                 'SPM, REST, DARTEL',
                                 'MATLAB',
                                 'v2.3_130615'],
                     'niak' : ['Neuroimaging analysis kit',
                               'MINC, PSOM',
                               'GNU Octave, MATLAB',
                               'v0.7.1']}

    # Set up lists of keys
    pnames = [pn for pn in pipeline_dict.iterkeys()]
    # Get next derivative id
    deriv_id = return_next_pk(cursor)
    # Timestamp
    timestamp = str(time.ctime(time.time()))
    # Get the filepath and split it
    fpath = url_path
    fstats = fpath.split('/')[4:]
    # Get the file statistics (pipeline, strategy)
    if len(fstats) >= 3:
        pname = fstats[0]
        name = fstats[2]
        aname = name.split('_')[1]
    else:
        print len(fstats)
        print 'Not logging this file: ', f
        continue
    if pname in pnames and 'rois' in name:
        # Grab atlas name from atlas dictionary
        atlas = atlas_dict[aname][0]
        roi = str(atlas_dict[aname][1])
        roidesc = 'Contains ROIs from the %s atlas' % atlas
        # Grab pipeline dictionary values
        pdesc = pipeline_dict[pname][0]
        ptools = pipeline_dict[pname][1]
        ptype = pipeline_dict[pname][2]
        pver = pipeline_dict[pname][3]
        # Measure, s3 path, strategy, template
        measure_name = 'image'
        s3_path = prefix + fpath
        strategy = fstats[1]
        template = 'MNI152'
        # Get ids of subject
        fname = fstats[-1]
        # Try and find a subject ID in the name
        try:
            sub_id = find_subid(fname)
        # If it couldn't find a subid, move on
        except:
            continue
        # Get dataset id and guid
        datasetid, guid = return_datasetid_guid(cursor,sub_id)
        # Insert the data
        cursor.execute(cmd,
                       col_1=deriv_id,
                       col_2=roi,
                       col_3=pname,
                       col_4=ptype,
                       col_5=ptools,
                       col_6=pver,
                       col_7=pdesc,
                       col_8=name,
                       col_9=measure_name,
                       col_10=timestamp,
                       col_11=s3_path,
                       col_12=template,
                       col_13=guid,
                       col_14=datasetid,
                       col_15=roidesc,
                       col_16=strategy,
                       col_17=atlas)
        # Print update
        print 'done: ', f
        print deriv_id
    elif pname in pnames and 'rois' not in name:
        pdesc = pipeline_dict[pname][0]
        ptools = pipeline_dict[pname][1]
        ptype = pipeline_dict[pname][2]
        pver = pipeline_dict[pname][3]
        # Measure, name, s3 path, template
        measure_name = 'image'
        s3_path = prefix + fpath
        template = 'MNI152'
        strategy = fstats[1]
        # Get ids of subject
        fname = fstats[-1]
        sub_id = find_subid(fname)
        # If it couldn't find a subid, move on
        if not sub_id:
            continue
        ids_cmd = '''
                  select id, guid from abide_subjects
                  where
                  sub_id = :arg_1
                  '''
        cursor.execute(ids_cmd, arg_1=sub_id)
        res = cursor.fetchall()[0]
        datasetid = res[0]
        guid = res[1]
        roi = 'Extracted brain'
        roidesc = 'Extracted brain registered to MNI space'
        # Insert the data
        cursor.execute(cmd,
                       col_1=deriv_id,
                       col_2=roi,
                       col_3=pname,
                       col_4=ptype,
                       col_5=ptools,
                       col_6=pver,
                       col_7=pdesc,
                       col_8=name,
                       col_9=measure_name,
                       col_10=timestamp,
                       col_11=s3_path,
                       col_12=template,
                       col_13=guid,
                       col_14=datasetid,
                       col_15=roidesc,
                       col_16=strategy)
        # Print update
        print 'done: ', f
        print deriv_id
    else:
        print 'skipping: ', f

    # Commit changes
    cursor.execute('commit')
