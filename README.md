This repository contains the code used to preprocess data from the NDAR database and upload the results/log files to tables in the database.

act_interface.py - Nipype interface made to work with the ANTs cortical thickness extraction script found here [antsCorticalThickness.sh](https://raw.githubusercontent.com/stnava/ANTs/master/Scripts/antsCorticalThickness.sh)
ndar_act_cluster.py - Streamlined script to execute the nipype workflow from the interface defined in act_interface.py and then upload the results and log files to the NDAR database. This script is designed to be launched from a cluster of C-PAC AMI's on AWS using the Sun Grid Engine job scheduler.
ndar_run.sge - Bash script to use to submit the ndar_act_cluster.py script in parallel over a cluster of nodes.
ndar_unpack - Bash-executable Python script which will download and extract imaging data from the NDAR database. Originally cloned from [here](https://raw.githubusercontent.com/chaselgrove/ndar/master/ndar_unpack/ndar_unpack), but slightly modified to add untar-ing functionality.

The OASIS template data files can be acquired from [Mindboggle](http://mindboggle.info) using this [link](http://mindboggle.info/data/templates/atropos/OASIS-30_Atropos_template.tar.gz) and this [link](http://mindboggle.info/data/atlases/jointfusion/OASIS-TRT-20_jointfusion_DKT31_CMA_labels_in_OASIS-30.nii.gz).

* Note the fetch_creds module needed for this database is a custom-module which returns AWS and RDS Oracle DB sensitive information. One can replace the variables this module returns with their own keys and database log-in information to use this functionality.

Python depenencies:
Boto - Python module for interacting with Amazon Web Services
cx_Oracle - Python module for interacting with Oracle databases
Nipype - Python module for Neuroimaging data analysis pipelines
PyYaml - Python module for parsing and emitting Yaml files

