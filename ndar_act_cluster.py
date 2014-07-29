# Import packages
from act_interface import antsCorticalThickness
import boto
import cx_Oracle
import fetch_creds
import logging
import nipype.interfaces.io as nio
import nipype.pipeline.engine as pe
from nipype.interfaces.ants import ApplyTransforms
import nipype.interfaces.utility as util
from nipype.interfaces.utility import Function
from nipype import logging as np_logging
from nipype import config
import os
import re
import subprocess
import sys
import time
import yaml


# Add result_stats database record
def add_db_record(img03_id, wf_status, extract_status, log_path, 
                  nifti_path, roi_path):
    # Grab the highest rs_id and increment
    rs_id = get_next_pk('results_stats','rs_id')
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
    roi_list = []
    # Open the roi txt file and read in to lists
    with open(roi_txt_path) as f:
        for i,line in enumerate(f):
            roi_list.append(line.split())
        key = roi_list[0][2:]
        val = roi_list[1][2:]
    # Form a dictionary from the lists
    roi_dict = dict(zip(key,val))
    return roi_dict


# Get next primary key id
def get_next_pk(table, pk_id):
    cmd = 'select max(%s) from %s' %(pk_id, table)
    cursor.execute(cmd)
    res = cursor.fetchall()[0][0]
    # If it has a value, increment and return
    if res:
        pk_id = int(res) + 1
    # Otherwise, consider this the first entry, return 1
    else:
        pk_id = 1
    return pk_id


# Function to load the ROIS to the unorm'd database
def insert_normd(img03_id, roi_dic=None, s3_path=None):
    # Get the pipeline id
    pipeline_name = 'ndar_act_workflow.py'
    pipeline_cmd = '''
                   select id from pipelines
                   where name = :arg_1
                   '''
    cursor.execute(pipeline_cmd, arg_1 = pipeline_name)
    pipeline_id = int(cursor.fetchall()[0][0])
    guid_find = '''
                select subjectkey
                from nitrc_image03
                where image03_id = :arg_1
                '''
    cursor.execute(guid_find, arg_1 = img03_id)
    guid = cursor.fetchall()[0][0]
    # If we're doing ROI means, enter them via dictionary
    if roi_dic:
        # Derivative name
        deriv_name = 'cortical thickness'
        # Get next deriv_id here
        deriv_id = get_next_pk('derivatives','id') 
        # Command string
        cmd = '''
              insert into %s
              (id, name, roiid, pipelineid, measureid, datasetid, value, 
               timestamp, units, guid)
              values
              (:col_1, :col_2, :col_3, :col_4, :col_5, :col_6, :col_7, 
               :col_8, :col_9, :col_10)
              '''
        # Measure name and ID
        measure_name = 'mean'
        measure_cmd = '''
                      select id from measures
                      where name = :arg_1
                      '''
        cursor.execute(measure_cmd, arg_1 = measure_name)
        measure_id = int(cursor.fetchall()[0][0])
        # Units
        units = 'mm'
        # Iterate through dictionary
        for k,v in roi_dic.iteritems():
            # Timestamp
            timestamp = str(time.ctime(time.time()))
            # Get ROI number
            roi = k.split('Mean_')[1]
            roi_cmd = '''
                      select id from rois
                      where name = :arg_1
                      '''
            cursor.execute(roi_cmd, arg_1 = roi)
            roi_id = int(cursor.fetchall()[0][0])
            # Value
            value = float(v)
            # Execute insert command
            cursor.execute(cmd % 'derivatives',
                           col_1 = deriv_id,
                           col_2 = deriv_name,
                           col_3 = roi_id,
                           col_4 = pipeline_id,
                           col_5 = measure_id,
                           col_6 = img03_id,
                           col_7 = value,
                           col_8 = timestamp,
                           col_9 = units,
                           col_10 = guid)
            # ...and increment the primary key
            deriv_id += 1
    # Otherwise, insert nifti file derivative
    if s3_path:
        # Derivative name
        deriv_name = 'Normalized cortical thickness image'
        # Measure name and ID
        measure_name = 'image'
        measure_cmd = '''
                      select id from measures
                      where name = :arg_1
                      '''
        cursor.execute(measure_cmd, arg_1 = measure_name)
        measure_id = int(cursor.fetchall()[0][0])
        # Get next deriv_id here
        deriv_id = get_next_pk('img_derivatives','id') 
        cmd = '''
              insert into %s
              (id, name, roiid, pipelineid, measureid, datasetid, s3_path, 
               timestamp, template, guid)
              values
              (:col_1, :col_2, :col_3, :col_4, :col_5, :col_6, :col_7, 
               :col_8, :col_9, :col_10)
              '''
        # Timestamp
        timestamp = str(time.ctime(time.time()))
        # Pipeline desc and measure fields
        pipeline_desc = 'compute image of cortical thickness of extracted '\
                        'and normalized brain'
        measure_name = 'image'
        units = 'filepath'
        roi_cmd = '''
                  select id from rois
                  where name = :arg_1
                  '''
        cursor.execute(roi_cmd, arg_1 = 'Grey matter')
        roi_id = int(cursor.fetchall()[0][0])
        template = 'OASIS-30_Atropos Template'
        # Execute insert command
        cursor.execute(cmd % 'img_derivatives',
                       col_1 = deriv_id,
                       col_2 = deriv_name,
                       col_3 = roi_id,
                       col_4 = pipeline_id,
                       col_5 = measure_id,
                       col_6 = img03_id,
                       col_7 = s3_path,
                       col_8 = timestamp,
                       col_9 = template,
                       col_10 = guid)
        # ...and increment the primary key
        deriv_id += 1
    # And commit changes
    cursor.execute('commit')


# Function to load the ROIS to the unorm'd database
def insert_unormd(img03_id, roi_dic=None, s3_path=None):
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
    cursor.execute(get_guid_cmd, arg_1=int(img03_id))
    guid = cursor.fetchall()[0][0]
    # If flag is set, insert ROI means 
    if roi_dic:
        deriv_name = 'cortical thickness'
        # Get next deriv_id here
        deriv_id = get_next_pk('derivatives_unormd','id') 
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
        deriv_id = get_next_pk('img_derivatives_unormd','id') 
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


# Mean ROI stats function
def roi_func(mask, thickness_normd):
    import os
    cmd = '3dROIstats -mask ' + mask + ' ' + thickness_normd + ' > ' + os.getcwd() + '/ROIstats.txt'
    os.system(cmd)
    roi_stats_file = os.path.join(os.getcwd(), 'ROIstats.txt')
    return roi_stats_file


# Setup log file
def setup_logger(logger_name, log_file, level=logging.INFO):
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
    from boto.s3.key import Key
    for (f,s) in zip(up_files,s3_files):
        k = Key(aws_bucket)
        k.key = s
        k.set_contents_from_filename(f)


# Start timing
start = time.time()

# --- Define path constants ---
# Oasis template paths
oasis_path = '/data/OASIS-30_Atropos_template/'
oasis_trt_20 = oasis_path + 'OASIS-TRT-20_jointfusion_DKT31_CMA_labels_in_OASIS-30.nii'
oasis_T_template0 = oasis_path + 'T_template0.nii.gz'
oasis_roi_yaml = oasis_path + 'oasis_roi_map.yml'
# Set subject list yaml file
base_path = '/data/act_run/'
yaml_file = base_path + 'anat_ids.yml'

# --- Setup s3 bucket connection for uploading ---
# Set AWS keys
aws_access_key_id, aws_secret_access_key = fetch_creds.return_aws_vars()
# Setup connection
conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
bucket = conn.get_bucket('ndar-data')

# --- Connect to AWS RDS Oracle MINDAR Database ---
# User info
user, passwd, host, port, sid = fetch_creds.return_rds_vars()
# Create dsn (data source name) and connect
dsn = cx_Oracle.makedsn(host,port,sid)
conn = cx_Oracle.connect(user,passwd,dsn)
# Create cursor
cursor = conn.cursor()

# --- Load in yml file data ---
# Load in subject list
inlist = yaml.load(open(yaml_file,'r'))
# Load in OASIS ROI map
oasis_roi_map = yaml.load(open(oasis_roi_yaml,'r'))

# --- Get subject from list ---
# Get job ID from SGE as subject index
#idx = 2
idx = int(sys.argv[1]) - 1
# Get subject info
subject = inlist[idx]
img03_id_str = str(subject[0])
s3_path = subject[1]
# Change bucket name to always be 'NDAR_Central' (caps-sensitive)
s3_list = s3_path.split('/')
s3_list[2] = 'NDAR_Central'
s3_path = '/'.join(s3_list)

# --- Set up log file ---
log_file = base_path + 'logs/' + img03_id_str + '.log'
setup_logger('log1', log_file)
ndar_log = logging.getLogger('log1')
# Log input image stats
ndar_log.info('-------- SGE TASK ID #%d --------' % (idx+1))
ndar_log.info('Start time: %s ' % time.ctime(start))
ndar_log.info('Input S3 path: %s' % s3_path)
ndar_log.info('Input IMAGE03 ID: %s' % img03_id_str)

# --- Search results_stats table for previous entries of that img03_id ---
cmd = '''
      select rs_id, wf_status
      from results_stats
      where img03_id = :arg_1
      '''
cursor.execute(cmd,arg_1=int(img03_id_str))
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
nifti_file = base_path + 'inputs/' + img03_id_str + '.nii.gz'
# Execute ndar_unpack for that subject
cmd = '/data/ndar_unpack'
if not os.path.exists(nifti_file):
    cmd_list = [cmd, '--aws-access-key-id', aws_access_key_id, 
                '--aws-secret-access-key', aws_secret_access_key, 
                '-v', nifti_file,s3_path]
    cmd_str = ' '.join(cmd_list)
    ndar_log.info('Executing command: %s ' % cmd_str)
    p = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, 
                         stderr=subprocess.STDOUT)
    p.wait()
    stdout, stderr = p.communicate()
    # Script output needs to be reversed back in order
#     stdlist = stdout.split('\n')
#     stdlist.reverse()
#     cmd_out = '\n'.join(stdlist)
#     ndar_log.info('ndar_unpack response:')
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
    add_db_record(img03_id_str, 'N/A', extract_status_str, 
                  'https://s3.amazonaws.com/ndar-data/' + s3_log_path, 'N/A', 'N/A')
    # And quit
    sys.exit()

# --- Image file successfully extracted, setup workflow for processing ---
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
thickness.inputs.input_skull = nifti_file #-a
thickness.inputs.template = oasis_path + 'T_template0.nii.gz' #-e
thickness.inputs.brain_prob_mask = oasis_path + 'T_template0_BrainCerebellumProbabilityMask.nii.gz'  #-m
thickness.inputs.brain_seg_priors = oasis_path + 'Priors2/priors%d.nii.gz'  #-p
thickness.inputs.intensity_template = oasis_path + 'T_template0_BrainCerebellum.nii.gz'  #-t
thickness.inputs.extraction_registration_mask = oasis_path + 'T_template0_BrainCerebellumExtractionMask.nii.gz'  #-f
thickness.inputs.out_prefix = 'OUTPUT_' #-o
thickness.inputs.keep_intermediate_files = 0 #-k
# Node to run ANTs 3dROIStats
ROIstats = pe.Node(util.Function(input_names=['mask','thickness_normd'], 
                                 output_names=['roi_stats_file'], 
                                 function=roi_func),
                   name='ROIstats')
wf.connect(thickness, 'cortical_thickness_normalized', ROIstats, 'thickness_normd')
ROIstats.inputs.mask = oasis_trt_20
# Create datasink node
datasink = pe.Node(nio.DataSink(), name='sinker')
datasink.inputs.base_directory = wf_base_dir

# --- Connect the nodes to form the workflow ---
# Connect thickness outputs to datasink
wf.connect(thickness, 'brain_extraction_mask', datasink, 'output.@brain_extr_mask')
wf.connect(thickness, 'brain_segmentation', datasink, 'output.@brain_seg')
wf.connect(thickness, 'brain_segmentation_N4', datasink, 'output.@brain_seg_N4')
wf.connect(thickness, 'brain_segmentation_posteriors_1', datasink, 'output.@brain_seg_post_1')
wf.connect(thickness, 'brain_segmentation_posteriors_2', datasink, 'output.@brain_seg_post_2')
wf.connect(thickness, 'brain_segmentation_posteriors_3', datasink, 'output.@brain_seg_post_3')
wf.connect(thickness, 'brain_segmentation_posteriors_4', datasink, 'output.@brain_seg_post_4')
wf.connect(thickness, 'brain_segmentation_posteriors_5', datasink, 'output.@brain_seg_post_5')
wf.connect(thickness, 'brain_segmentation_posteriors_6', datasink, 'output.@brain_seg_post_6')
wf.connect(thickness, 'cortical_thickness', datasink, 'output.@cortical_thickness')
wf.connect(thickness, 'cortical_thickness_normalized', datasink,'output.@cortical_thickness_normalized')
# Connect ROI stats output text file to datasink
wf.connect(ROIstats, 'roi_stats_file', datasink, 'output.@ROIstats')
# Setup crashfile directory and logging
wf.config['execution'] = {'hash_method': 'timestamp', 'crashdump_dir': crash_dir}
config.update_config({'logging': {'log_directory': log_dir, 'log_to_file': True}})
np_logging.update_logging(config)

# --- Run the workflow ---
wf_status = 0
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
    up_nifti_path = wf_base_dir + '/output/OUTPUT_CorticalThicknessNormalizedToTemplate.nii.gz'
    up_roi_path = wf_base_dir + '/output/ROIstats.txt'
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
        insert_unormd(img03_id_str, roi_dic=sub_roi_dic)
        insert_normd(img03_id_str, roi_dic=sub_roi_dic)
        # Insert the act nifti into the unorm'd and norm'd databases
        insert_unormd(img03_id_str, s3_path=full_s3_nifti_path)
        insert_normd(img03_id_str, s3_path=full_s3_nifti_path)
    except Exception, e:
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
add_db_record(img03_id_str, 
              wf_status_str, 
              extract_status_str, 
              's3://ndar-data/'+s3_log_path, 
              full_s3_nifti_path, 
              full_s3_roi_path)
