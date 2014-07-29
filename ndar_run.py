# Import packages
import os
import yaml
import cx_Oracle
import boto
import boto.s3.connection

# AWS account info
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
# User info
user = os.environ.get('DB_USER')
password = os.environ.get('DB_PASSWD')
# Database info
host = os.environ.get('DB_HOST')
port = os.environ.get('DB_PORT')
sid = os.environ.get('DB_SID')

base_path = '/home/dclark/Documents/data/NDAR_Data/'
yaml_file = base_path + 'ndar_subjects.yml'

def fetch_data(user, password, host, port, sid):

    # Create dsn (data source name) and connect
    dsn = cx_Oracle.makedsn(host,port,sid)
    conn = cx_Oracle.connect(user,password,dsn)
    
    # Create cursor
    cursor = conn.cursor()
    
    # Get the subjects from image03
    cmd = '''
          select image_file, image03_id
          from image03
          '''
    # Execute query and fetch result
    cursor.execute(cmd)
    sublist = cursor.fetchall()
    
    # Write subject list to yaml file
    with open(yaml_file,'w') as f:
        f.write(yaml.dump(sublist))
    f.close()

# Load yaml file and download subjects
inlist = yaml.load(open(yaml_file,'r'))
no_subjects = 100

# Create lists of extracted and non-extracted images
extracted_subs = []
unextracted_subs = []
for sub in inlist[200:400]:
    img_id = str(sub[1])
    nifti_name = base_path + 'run1/' + img_id + '.nii.gz'
    s3_path = sub[0]
    cmd = base_path + 'ndar_unpack -v ' + nifti_name + ' ' + s3_path + \
          ' --aws-access-key-id ' + aws_access_key_id + \
          ' --aws-secret-access-key ' + aws_secret_access_key
    #print cmd
    if not os.path.exists(nifti_name):
        os.system(cmd)

    # Check if extraction was successful
    if os.path.exists(nifti_name):
        extracted_subs.append(img_id)
    else:
        unextracted_subs.append(img_id)

# Yaml files to store subjects which were extracted or not
ext_yaml = base_path + 'extracted_subs.yml'
un_yaml = base_path + 'unextracted_subs.yml'

# And write to them
with open(ext_yaml,'w') as f:
    f.write(yaml.dump(extracted_subs))
f.close()
with open(un_yaml,'w') as f:
    f.write(yaml.dump(unextracted_subs))
f.close()

