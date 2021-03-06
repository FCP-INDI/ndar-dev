# ndar_act_run.py
#
# Author: Daniel Clark, 2014

'''
This module contains functions which preprocess NDAR T1-weighted
anatomical images using ANTs to extract a cortical thickness estimation
image. This image is then analyzed over a set of ROI's to produce
average cortical thickness estimations into a text file. These results
are then uploaded to an AWS-hosted Oracle database via miNDAR.
'''

# Add result_stats database record
def add_db_record(cursor, img03_id, wf_status, extract_status, log_path, 
                  nifti_path, roi_path):
    '''
    Method to add a record to the RESULTS_STATS table on miNDAR

    Parameters
    ----------
    cursor : OracleCursor
        a cx_Oracle cursor object which is used to query and modify an
        Oracle database
    img03_id : integer
        image03_id of the image result to be inserted
    wf_status : string
        string which indicates if the nipype workflow passed or failed
        the run (e.g. 'PASS')
    extract_status : string
        string which indicates if the image was able to be downloaded
        and extracted to nifti format
    log_path : string
        filepath to the log file that was uploaded to S3
    nifti_path : string
        filepath to the nifti image file that was uploaded to S3
    roi_path : string
        filepath to the ROIstats.txt file that was uploaded to S3


    Returns
    -------
    None
        The function doesn't return any value, it inserts an entry into
        the RESULTS_STATS table.
    '''
    
    # Import packages
    import time

    # Grab the highest rs_id and increment
    rs_id = get_next_pk(cursor, 'results_stats','rs_id')
    # Form the insert command
    cmd = '''
          insert into results_stats
          (rs_id, img03_id, wf_status, extract_status, log_path, nifti_path,
           roi_path, timestamp)
          values
          (:col1, :col2, :col3, :col4, :col5, :col6, :col7, :col8)
          '''
    timestamp = time.ctime(time.time())
    cursor.execute(cmd, 
                   col1 = rs_id, 
                   col2 = int(img03_id), 
                   col3 = wf_status, 
                   col4 = extract_status, 
                   col5 = log_path, 
                   col6 = nifti_path,
                   col7 = roi_path, 
                   col8 = timestamp)
    cursor.execute('commit')


# Function to create ROI values dictionary
def create_roi_dic(roi_txt_path):
    '''
    Method to create a python dictionary from the ROIstats.txt file
    generated by the 3dROIstats function

    Parameters
    ----------
    roi_txt_path : string
        filepath to the ROIstats.txt file

    Returns
    -------
    roi_dict : dictionary {str : float}
        dictionary of string keys with corresponding float values,
        corresponding to the ROI label and value, respectively
    '''

    # Init variables
    roi_list = []

    # Open the roi txt file and read in to lists
    with open(roi_txt_path) as f:
        for i,line in enumerate(f):
            roi_list.append(line.split())
        key = roi_list[0][2:]
        val = roi_list[1][2:]
    # Form a dictionary from the lists
    roi_dict = dict(zip(key,val))

    # Return the dictionary
    return roi_dict


# Create the ACT nipype workflow
def create_workflow(base_path, img03_id_str, input_skull, oasis_path):
    '''
    Method to create the nipype workflow that is executed for
    preprocessing the data

    Parameters
    ----------
    base_path : string
        filepath to the base directory to create the workflow folders
    img03_id_str : string
        string of the image03_id of the input subject to process
    input_skull : string
        filepath to the input file to run antsCorticalThickness.sh on
    oasis_path : string
        filepath to the oasis

    Returns
    -------
    wf : nipype.pipeline.engine.Workflow instance
        the workflow to be ran for preprocessing
    '''

    # Import packages
    from act_interface import antsCorticalThickness
    import nipype.interfaces.io as nio
    import nipype.pipeline.engine as pe
    import nipype.interfaces.utility as util
    from nipype.interfaces.utility import Function
    from nipype import logging as np_logging
    from nipype import config

    # Init variables
    oasis_trt_20 = oasis_path + 'OASIS-TRT-20_jointfusion_DKT31_CMA_labels_in_OASIS-30.nii'
    
    # Setup nipype workflow
    wf_base_dir = base_path + 'work-dirs/' + img03_id_str
    if not os.path.exists(wf_base_dir):
        os.makedirs(wf_base_dir)
    wf = pe.Workflow(name='thickness_workflow')
    wf.base_dir = wf_base_dir
    
    # Crash directory
    crash_dir = wf_base_dir + '/crashes/'
    log_dir = wf_base_dir
    
    # Define antsCorticalThickness node
    thickness = pe.Node(antsCorticalThickness(), name='thickness')
    
    # Set antsCorticalThickness inputs
    thickness.inputs.dimension = 3
    thickness.inputs.segmentation_iterations = 1
    thickness.inputs.segmentation_weight = 0.25
    thickness.inputs.input_skull = input_skull #-a
    thickness.inputs.template = oasis_path + 'T_template0.nii.gz' #-e
    thickness.inputs.brain_prob_mask = oasis_path + \
                                       'T_template0_BrainCerebellumProbabilityMask.nii.gz'  #-m
    thickness.inputs.brain_seg_priors = oasis_path + \
                                        'Priors2/priors%d.nii.gz'  #-p
    thickness.inputs.intensity_template = oasis_path + \
                                          'T_template0_BrainCerebellum.nii.gz'  #-t
    thickness.inputs.extraction_registration_mask = oasis_path + \
                                                    'T_template0_BrainCerebellumExtractionMask.nii.gz'  #-f
    thickness.inputs.out_prefix = 'OUTPUT_' #-o
    thickness.inputs.keep_intermediate_files = 0 #-k
    
    # Node to run ANTs 3dROIStats
    ROIstats = pe.Node(util.Function(input_names=['mask','thickness_normd'], 
                                     output_names=['roi_stats_file'], 
                                     function=roi_func),
                       name='ROIstats')
    wf.connect(thickness, 'cortical_thickness_normalized', 
               ROIstats, 'thickness_normd')
    ROIstats.inputs.mask = oasis_trt_20
    
    # Create datasink node
    datasink = pe.Node(nio.DataSink(), name='sinker')
    datasink.inputs.base_directory = wf_base_dir

    # Connect thickness outputs to datasink
    wf.connect(thickness, 'brain_extraction_mask', 
               datasink, 'output.@brain_extr_mask')
    wf.connect(thickness, 'brain_segmentation', 
               datasink, 'output.@brain_seg')
    wf.connect(thickness, 'brain_segmentation_N4', 
               datasink, 'output.@brain_seg_N4')
    wf.connect(thickness, 'brain_segmentation_posteriors_1', 
               datasink, 'output.@brain_seg_post_1')
    wf.connect(thickness, 'brain_segmentation_posteriors_2', 
               datasink, 'output.@brain_seg_post_2')
    wf.connect(thickness, 'brain_segmentation_posteriors_3', 
               datasink, 'output.@brain_seg_post_3')
    wf.connect(thickness, 'brain_segmentation_posteriors_4', 
               datasink, 'output.@brain_seg_post_4')
    wf.connect(thickness, 'brain_segmentation_posteriors_5', 
               datasink, 'output.@brain_seg_post_5')
    wf.connect(thickness, 'brain_segmentation_posteriors_6', 
               datasink, 'output.@brain_seg_post_6')
    wf.connect(thickness, 'cortical_thickness', 
               datasink, 'output.@cortical_thickness')
    wf.connect(thickness, 'cortical_thickness_normalized', 
               datasink,'output.@cortical_thickness_normalized')
    # Connect ROI stats output text file to datasink
    wf.connect(ROIstats, 'roi_stats_file', datasink, 'output.@ROIstats')
    
    # Setup crashfile directory and logging
    wf.config['execution'] = {'hash_method': 'timestamp', 
                              'crashdump_dir': crash_dir}
    config.update_config({'logging': {'log_directory': log_dir, 
                                      'log_to_file': True}})
    np_logging.update_logging(config)

    # Return the workflow
    return wf, crash_dir

    
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


# Function to load the ROIS to the unorm'd database
def insert_unormd(cursor, img03_id_str, roi_dic=None, s3_path=None):
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
    pk_id : string
        field name of the column that contains the primary keys

    Returns
    -------
    None
        The function doesn't return any value, it inserts an entry into
        the un-normalized database tables
    '''
    # Import packages
    import time
    oasis_path = '/data/OASIS-30_Atropos_template/'
    oasis_roi_yaml = oasis_path + 'oasis_roi_map.yml'
    # Load in OASIS ROI map
    oasis_roi_map = yaml.load(open(oasis_roi_yaml,'r'))

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
    if roi_dic:
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
        for k,v in roi_dic.iteritems():
            # Timestamp
            timestamp = str(time.ctime(time.time()))
            # Get ROI number and name from dictionaries
            roi = k.split('Mean_')[1]
            roi_name = oasis_roi_map[k]
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
    if s3_path:
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
        print 'made it to cursor command on s3 upload'
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


# Mean ROI stats function
def roi_func(mask, thickness_normd):
    '''
    Method to run 3dROIstats on an input image, thickness_normd, using
    a mask, mask The output is written to the current working directory
    as 'ROIstats.txt'

    Parameters
    ----------
    mask : string
        filepath to the mask to be used
    thickness_normd : string
        filepath to the input image

    Returns
    -------
    roi_stats_file : string
        the filepath to the generated ROIstats.txt file
    '''

    # Import packages
    import os

    # Set command and execute
    cmd = '3dROIstats -mask ' + mask + ' ' + thickness_normd + ' > ' + os.getcwd() + '/ROIstats.txt'
    os.system(cmd)

    # Get the output
    roi_stats_file = os.path.join(os.getcwd(), 'ROIstats.txt')

    # Return the filepath to the output
    return roi_stats_file

# Setup log file
def setup_logger(logger_name, log_file, level):
    '''
    Docstring for setup_logger
    '''

    # Import packages
    import logging

    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s : %(message)s')
    fileHandler = logging.FileHandler(log_file, mode='w')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    
    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler)


# Add result_stats database record
def upload_to_s3(aws_bucket, up_files, s3_files):
    '''
    Docstring for upload_to_s3
    '''

    from boto.s3.key import Key
    for (f,s) in zip(up_files,s3_files):
        k = Key(aws_bucket)
        k.key = s
        k.set_contents_from_filename(f)

# Main routine
def main(sub_list, sub_idx):
    '''
    Method to preprocess a subject's image (nifti) data using ANTs
    and upload it to a miNDAR database. First argument to script
    specifies index of subject to process of subject list, which is
    
    Parameters
    ----------
    sub_list : string
        filepath to a yaml file which contains a python list of tuples
        each tuple in the list is of the form (img03_id, s3_path),
        where img03_id is an integer corresponding to the image03_id
        of the image and the s3_path is a string corresponding to the
        path of the image on S3.
        e.g. (123, 's3://NDAR_Bucket/subject/image01.nii')
    sub_idx : integer
        index of subject to process from the sub_list yaml file

    Returns
    -------
    None
        The function doesn't return any value, it processes and uploads
        data to S3 and creates a log file of the overall progress.
    '''

    # Import packages
    import boto
    import cx_Oracle
    import fetch_creds
    import logging
    from nipype import logging as np_logging
    from nipype import config
    import os
    import re
    import subprocess
    import sys
    import time
    import yaml

    # Start timing
    start = time.time()

    # Init variables
    base_path = '/data/act_run/'
    creds_path = '/data/creds/Daniels_credentials.csv'
    # Oasis template paths
    oasis_path = '/data/OASIS-30_Atropos_template/'
    oasis_roi_yaml = oasis_path + 'oasis_roi_map.yml'
    # Load in OASIS ROI map
    oasis_roi_map = yaml.load(open(oasis_roi_yaml,'r'))
    
    # Setup s3 bucket, RDS cursor connections for uploading
    aws_access_key_id, aws_secret_access_key = fetch_creds.return_aws_keys(creds_path)
    bucket = fetch_creds.return_bucket(creds_path, 'ndar-data')
    cursor = fetch_creds.return_cursor(creds_path)

    # Get subject info
    subject = sub_list[sub_idx-1]
    img03_id_str = str(subject[0])
    s3_path = subject[1]
    
    # Change bucket name to always be 'NDAR_Central' (caps-sensitive)
    s3_list = s3_path.split('/')
    s3_list[2] = 'NDAR_Central'
    s3_path = '/'.join(s3_list)

    # --- Set up log file ---
    log_file = base_path + 'logs/' + img03_id_str + '.log'
    setup_logger('log1', log_file, logging.INFO)
    ndar_log = logging.getLogger('log1')
    # Log input image stats
    ndar_log.info('-------- RUNNING SUBJECT NO. #%d --------' % (sub_idx))
    ndar_log.info('Start time: %s ' % time.ctime(start))
    ndar_log.info('Input S3 path: %s' % s3_path)
    ndar_log.info('Input IMAGE03 ID: %s' % img03_id_str)

    # --- Search results_stats table for previous entries of that img03_id ---
    cmd = '''
          select rs_id, wf_status
          from results_stats
          where img03_id = :arg_1
          '''
    cursor.execute(cmd, arg_1=int(img03_id_str))
    result = cursor.fetchall()
    # If the record already exists, check to see if it was successful
    wkflow_flag = 0
    for record in result:
        wkflow_status = record[1]
        if wkflow_status == 'PASS':
            wkflow_flag = 1
            rs_id = record[0]
    # Log if already found and exit
    if wkflow_flag:
        ndar_log.info('Image already successfully ran, found at RS_ID: %d' % rs_id)
        sys.exit()

    # --- Download and extract data from NDAR_Central S3 bucket ---
    nifti_file = base_path + 'inputs-ef/' + img03_id_str + '.nii.gz'
    # Execute ndar_unpack for that subject
    cmd = './ndar_unpack'
    if not os.path.exists(nifti_file):
        cmd_list = [cmd, '--aws-access-key-id', aws_access_key_id, 
                    '--aws-secret-access-key', aws_secret_access_key, 
                    '-v', nifti_file, s3_path]
        cmd_str = ' '.join(cmd_list)
        ndar_log.info('Executing command: %s ' % cmd_str)
        p = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, 
                             stderr=subprocess.STDOUT)
        p.wait()
        stdout, stderr = p.communicate()
        ndar_log.info(stdout)
    else:
        ndar_log.info('Nifti file already present for IMAGE03 ID %s' % img03_id_str)
        ndar_log.info('ndar_unpack did not need to run')

    extract_status_str = 'PASS'
    # If file was never created, log and exit
    if not os.path.exists(nifti_file):
        ndar_log.info('File extraction FAILED for IMAGE03 ID %s' % img03_id_str)
        extract_status_str = 'FAIL'
        # Upload the log file
        time_str = time.strftime('%Y-%m-%d_%H%M-%S',time.localtime(time.time()))
        s3_filename = time_str + '_' + img03_id_str
        up_log_list = []
        s3_log_list = []
        s3_log_path = 'logs/' + s3_filename + '.log'
        up_log_list.append(log_file)
        s3_log_list.append(s3_log_path)
        upload_to_s3(bucket, up_log_list, s3_log_list)
        # Finally upload the record to the database
        add_db_record(cursor, img03_id_str, 'N/A', extract_status_str, 
                      'https://s3.amazonaws.com/ndar-data/' + s3_log_path, 'N/A', 'N/A')
        # And quit
        sys.exit()

    # Create the nipype workflow
    wf, crash_dir = create_workflow(base_path, img03_id_str, nifti_file, oasis_path)

    # --- Run the workflow ---
    wf_base_dir = base_path + 'work-dirs/' + img03_id_str
    up_nifti_path = wf_base_dir + \
                    '/output/OUTPUT_CorticalThicknessNormalizedToTemplate.nii.gz'
    up_roi_path = wf_base_dir + '/output/ROIstats.txt'
    if os.path.exists(up_nifti_path) and os.path.exists(up_roi_path):
        wf_status = 1
    else:
        wf_status = 0
    if wf_status == 0:
	    try:
                ndar_log.info('Running the workflow...')
                wf.run()
                # We're successful at this point, add it as a file to the completed path
                ndar_log.info('Workflow completed successfully for IMAGE03 ID %s' % img03_id_str)
                wf_status = 1
                finish_str = 'Finish time: %s'
	    # If the workflow run fails
	    except:
                ndar_log.info('ACT Workflow failed for IMAGE03 ID %s' % img03_id_str)
                finish_str = 'Crash time: %s'
    else:
        finish_str = 'Workflow did not need to run as files were already there: %s'

    # Log finish and total computation time
    fin = time.time()
    elapsed = (fin - start)/60
    ndar_log.info(finish_str % time.ctime(fin))
    ndar_log.info('Total time running IMAGE03 ID %s is: %s minutes' \
                  %(img03_id_str,str(elapsed)))

    up_list = []
    s3_list = []
    time_str = time.strftime('%Y-%m-%d_%H-%M-%S',time.localtime(fin))
    s3_filename = time_str + '_' + img03_id_str

    # If workflow completed succesfully
    if wf_status:
        # Define cloud data and status
        wf_status_str = 'PASS'
        s3_nifti_path = 'outputs/' + img03_id_str + '/' + img03_id_str + \
                        '_corticalthickness_normd.nii.gz'
        s3_roi_path = 'outputs/' + img03_id_str + '/' + img03_id_str + \
                      '_ROIstats.txt' 
        full_s3_nifti_path = 's3://ndar_data/' + s3_nifti_path
        full_s3_roi_path = 's3://ndar_data/' + s3_roi_path
        # Upload paths
        #wf_base_dir = base_path + 'work-dirs/' + img03_id_str
        #up_nifti_path = wf_base_dir + \
        #                '/output/OUTPUT_CorticalThicknessNormalizedToTemplate.nii.gz'
        #up_roi_path = wf_base_dir + '/output/ROIstats.txt'
        # Append upload/s3 lists with path names
        up_list.append(up_nifti_path)
        up_list.append(up_roi_path)
        s3_list.append(s3_nifti_path)
        s3_list.append(s3_roi_path)
        # Log nifti and roi files upload
        ndar_log.info('Uploading nifti and roi files...')
        # Create dictionary of ROIs for that subject
        sub_roi_dic = create_roi_dic(up_roi_path)
        try:
            # Insert the ROIs into the unorm'd and norm'd databases
            ndar_log.info('uploading rois...')
            print '----------------------------------'
            insert_unormd(cursor, img03_id_str, roi_dic=sub_roi_dic)
            ndar_log.info('uploading imgs...')
            # Insert the act nifti into the unorm'd and norm'd databases
            insert_unormd(cursor, img03_id_str, s3_path=full_s3_nifti_path)
        except:
            e = sys.exc_info()[0]
            ndar_log.info('Error inserting results to MINDAR, message: %s' % str(e))
            wf_status_str = 'Error inserting results into MINDAR database'
    # Otherwise, there were crash files, upload those
    else:
        # Define cloud data and status
        wf_status_str = 's3://ndar-data/crashes/' + s3_filename + '/'
        full_s3_nifti_path = 'N/A'
        full_s3_roi_path = 'N/A'
        # Find crash file names/paths
        for root, dirs, files in os.walk(crash_dir):
            root_path = os.path.abspath(root)
            crash_files = files
        # Append crash file and s3 path lists
        for f in crash_files:
            crash_path = root_path + '/' + f
            s3_crash_path = 'crashes/' + s3_filename + '/' + f
            up_list.append(crash_path)
            s3_list.append(s3_crash_path)
        # Log crash file upload 
        ndar_log.info('Uploading crash files into %s ...' % wf_status_str)

    # Call the upload function
    upload_to_s3(bucket, up_list, s3_list)
    ndar_log.info('Done')

    # Upload the log file
    up_log_list = []
    s3_log_list = []
    s3_log_path = 'logs/' + s3_filename + '.log'
    up_log_list.append(log_file)
    s3_log_list.append(s3_log_path)
    upload_to_s3(bucket, up_log_list, s3_log_list) 

    # Finally upload the record to the database
    add_db_record(cursor, 
                  img03_id_str, 
                  wf_status_str, 
                  extract_status_str, 
                  's3://ndar-data/'+s3_log_path, 
                  full_s3_nifti_path, 
                  full_s3_roi_path)

# Run main by default
if __name__ == '__main__':
    
    # Import packages
    import os
    import sys
    import yaml

    # Init variables
    yaml_file = os.path.abspath(sys.argv[1])
    sub_idx = int(sys.argv[2])

    # Load in subject list
    sub_list = yaml.load(open(yaml_file,'r'))

    # Execute main routine
    main(sub_list, sub_idx)
