# freesurfer_insert.py
#
# Author: Daniel Clark, 2014

'''
This module contains functions which populate the abide_img_results
table on the miNDAR database using ABIDE preprocessed data stored in
Amazon's S3 service.
'''

# Init global variables
# Pipeline info
pname = 'Freesurfer'
ptype = 'Executable, C++'
ptools = 'Tkmedit, Tksurfer'
pver = '5.1'
pdesc = 'Toolset for analysis and visualization of structural and functional brain imaging data'
strategy = 'Band-pass filtering, global signal regression'
template = 'native'
# Cursor command for inserting data
cmd = '''
      insert into abide_img_results
      (id, roi, pipelinename, pipelinetype,  pipelinetools, pipelineversion, 
      pipelinedescription, name, measurename, timestamp, s3_path, template, 
      guid, datasetid, roidescription, strategy, atlas, value, units)
      values
      (:col_1, :col_2, :col_3, :col_4, :col_5, :col_6, :col_7, :col_8,
      :col_9, :col_10, :col_11, :col_12, :col_13, :col_14, :col_15,
      :col_16, :col_17, :col_18, :col_19)
      '''

# Get the statistics of the freesurfer stats file and upload
def extract_upload_stats(cursor, url_path, subs_dir):
    '''
    Function which uses the recon-stats python package to extract stats
    info from the Freesurfer-generated. Note, for recon-stats to work
    properly, it needs the SUBJECTS_DIR variable to be set in the OS
    environment, as well as having all of the stats data organized as:

        $SUBJECTS_DIR/{sub_id}/stats/*.stats

    Parameters
    ----------
    cursor : OracleCursor
        a cx_Oracle cursor object which is used to query and modify an
        Oracle database
    sub_id : string
        a subject ID that is located in the $
    subs_dir : string
        filepath to the subjects directory (Freesurfer directory structure)

    Returns
    -------
    None
        The function doesn't return any value, it uploads data to the
        datasase using the input cursor and url_path
    '''

    # Import packages
    import insert_utils
    import os
    import recon_stats
    import time

    # Init variables
    os.environ['SUBJECTS_DIR'] = subs_dir
    # Get subject id from url_path
    fname = url_path.split('/')[-1]
    subid = insert_utils.find_subid(fname)
    subkey = fname.split(subid)[0] + subid
    # Get filename
    fkey = fname.split('_stats_')[-1]

    # Use the subid to create a recon_stats.Subject object
    s = recon_stats.Subject(subkey)
    # Parse through the stats files and get a list of measures
    s.get_measures(fkey)
    mlist = s.measures

    # Get next derivative id to insert
    deriv_id = insert_utils.return_next_pk(cursor)
    if 'aparc.stats' in fname:
        # Set atlas to parcellation atlas
        atlas = 'Desikan-Killiany Atlas'
    elif 'aparc.a2009s.stats' in fname:
        # Set atlas to parcellation atla
        atlas = 'Destrieux Atlas'
    elif 'BA.stats' in fname:
        atlas = 'Brodmann Atlas'
    else:
        atlas = ''

    # Now iterate through the measures list and insert into db
    for m in mlist:
        # Timestamp
        timestamp = str(time.ctime(time.time()))
        # Get measure info
        roi = ''
        roidesc = m.structure
        name = m.name()
        measure = m.measure
        s3_path = url_path
        datasetid, guid = insert_utils.return_datasetid_guid(cursor, subid)
        value = m.value
        units = m.units

        # Insert entry
        cursor.execute(cmd,
                       col_1=deriv_id,
                       col_2=roi,
                       col_3=pname,
                       col_4=ptype,
                       col_5=ptools,
                       col_6=pver,
                       col_7=pdesc,
                       col_8=name,
                       col_9=measure,
                       col_10=timestamp,
                       col_11=s3_path,
                       col_12=template,
                       col_13=guid,
                       col_14=datasetid,
                       col_15=roidesc,
                       col_16=strategy,
                       col_17=atlas,
                       col_18=value,
                       col_19=units)
        # Commit changes
        cursor.execute('commit')
        deriv_id += 1
        print deriv_id


# Select which kind of dat file info to use
def fetch_freesurfer_info(url_path):
    '''
    Method to populate initialize database field values for a new entry
    based on the file type (url_path) passed in.

    Parameters
    ----------
    url_path : string (url)
        URL address of the .dat file to parse and insert into miNDAR

    Returns
    -------
    beg : integer, default = None
        beginning index to read from the url_path file contents; used
        as inputs to the get_rois_from_dat function
    end : integer, default = None
        ending index to read from the url_path file contents; used
        as inputs to the get_rois_from_dat function
    name : string
        name of the neuro-imaging metric to be uploaded to database
    measure : string
        type of neuro-imaging metric to be uploaded to database
    template : string
        template used for measurement; either native or stereotaxic space
    units : string
        units of the neuro-imaging metric
    split_str : string, defaults = None
        the delimiter to split the url_path file contents by, per line

    '''

    # Init variables
    split_str = None
    beg = None
    end = None
    # Type of data
    folder = url_path.split('/')[-1]

    # Check if it's a dat file
    if 'surf' in folder:
        # If it has 'area' in the folder name
        if 'area' in folder:
            name = 'vertex-based surface areas'
            # Left or right hemisphere
            if 'lh' in folder:
                name = name + ' of left hemisphere'
            elif 'rh' in folder:
                name = name + ' of right hemisphere'
            else:
                raise ValueError, 'unexpected input %s, check url_path' % url_path
            # Set measurename field
            measure = 'area'
            template = 'native'
            units = 'mm^2'
        # If it has 'area' in the folder name
        elif 'curv' in folder:
            name = 'vertex-based surface curvatures'
            # Left or right hemisphere
            if 'lh' in folder:
                name = name + ' of left hemisphere'
            elif 'rh' in folder:
                name = name + ' of right hemisphere'
            else:
                raise ValueError, 'unexpected input %s, check url_path' % url_path
            # Set measurename field
            measure = 'curvature'
            template = 'native'
            units = 'mm^-1'
        # If it has 'area' in the folder name
        elif 'sulc' in folder:
            name = 'vertex-based sulcal depths'
            # Left or right hemisphere
            if 'lh' in folder:
                name = name + ' of left hemisphere'
            elif 'rh' in folder:
                name = name + ' of right hemisphere'
            else:
                raise ValueError, 'unexpected input %s, check url_path' % url_path
            # Set measurename field
            measure = 'distance'
            template = 'native'
            units = 'mm'
        # If it has 'area' in the folder name
        elif 'thickness' in folder:
            name = 'vertex-based cortical thickness'
            # Left or right hemisphere
            if 'lh' in folder:
                name = name + ' of left hemisphere'
            elif 'rh' in folder:
                name = name + ' of right hemisphere'
            else:
                raise ValueError, 'unexpected input %s, check url_path' % url_path
            # Set measurename field
            measure = 'distance'
            template = 'native'
            units = 'mm'
        # If it has 'area' in the folder name
        elif 'volume' in folder:
            name = 'vertex-based cortical gray matter volume'
            # Left or right hemisphere
            if 'lh' in folder:
                name = name + ' of left hemisphere'
            elif 'rh' in folder:
                name = name + ' of right hemisphere'
            else:
                raise ValueError, 'unexpected input %s, check url_path' % url_path
            # Set measurename field
            measure = 'volume'
            template = 'native'
            units = 'mm^-3'

    # Return the dat-specific info
    return beg, end, name, measure, template, units, split_str


# Insert lobe ROI values and total from freesurfer
def upload_results(cursor, url_path):
    '''
    Method to insert Freesurfer pipeline data from files
    into the abide_img_results table in miNDAR

    Parameters
    ----------
    cursor : OracleCursor
        a cx_Oracle cursor object which is used to query and modify an
        Oracle database
    url_path : string (url)
        URL address of the .dat file to parse and insert into miNDAR

    Returns
    -------
    None
        The function doesn't return any value, it uploads data to the
        datasase using the input cursor and url_path
    '''

    # Import packages
    import insert_utils
    import time
    import urllib

    # If it's a dat file, get the specific file's info
    if url_path.endswith('.stats'):
        extract_upload_stats(cursor, url_path)
    # Otherwise, it's a surface file
    else:
        # Init variables
        # Known field values for CIVET pipeline results
        deriv_id = insert_utils.return_next_pk(cursor)
        # S3 path
        s3_path = url_path
        # Get datasetid and guid
        fname = url_path.split('/')[-1]
        sub_id = insert_utils.find_subid(fname)
        datasetid, guid = insert_utils.return_datasetid_guid(cursor, sub_id)

        # Get the specific file's info
        print 'Analyzing %s...' % url_path
        beg, end, name, measure, template, units, split_str = fetch_freesurfer_info(url_path)
        # Timestamp
        timestamp = str(time.ctime(time.time()))
        # ROI description
        value = ''
        roi = ''
        roidesc = ''
        atlas = ''
        # Insert entry
        cursor.execute(cmd,
                       col_1=deriv_id,
                       col_2=roi,
                       col_3=pname,
                       col_4=ptype,
                       col_5=ptools,
                       col_6=pver,
                       col_7=pdesc,
                       col_8=name,
                       col_9=measure,
                       col_10=timestamp,
                       col_11=s3_path,
                       col_12=template,
                       col_13=guid,
                       col_14=datasetid,
                       col_15=roidesc,
                       col_16=strategy,
                       col_17=atlas,
                       col_18=value,
                       col_19=units)
        # Commit changes
        cursor.execute('commit')
        print deriv_id


    # Print done with that file and return
    print 'Done!'
    return
