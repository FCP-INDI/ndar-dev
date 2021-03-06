ndar-dev
========
This repository contains the code used to preprocess data from the NDAR database and upload the results/log files to tables in the database.

## Contents
- abide_upload - Folder with scripts to upload ABIDE data from the [Preprocessed Connectomes Project](http://preprocessed-connectomes-project.github.io/) to a miNDAR database instance table.
- act_interface.py - Nipype interface made to work with the ANTs cortical thickness extraction script found [here](https://raw.githubusercontent.com/stnava/ANTs/master/Scripts/antsCorticalThickness.sh)
- act_sublist_build.py - Template subject list builder script which will query the IMAGE03 database table and pull down a range of image03_id's and their corresponding S3 path entries to build a subject list. This subject list can then be used to run ndar_act_run.py for ANTs cortical thickness processing.
- aws_walkthrough.md - Instructions on how to use AWS EC2 to launch and interact with a C-PAC AMI.
- check_entries.py - A quality control script that analyzes results in the miNDAR tables and determines if they are complete or need to be modified/deleted.
- credentials_template.csv - A template for how the fetch_creds.py module expects in order to read in credentials and use them for python interfaces to various AWS services
- fetch_creds.py - A python module which reads in a csv file (e.g. credentials_template) and uses this information to create variables and objects used in interfacing with AWS via python.
- ndar_act_run.py - Streamlined script to execute the nipype workflow from the interface defined in act_interface.py and then upload the results and log files to the NDAR database. This script is designed to be launched from a cluster of C-PAC AMI's on AWS using the Sun Grid Engine job scheduler. It uses templates generated from data as part of the [OASIS project](http://www.oasis-brains.org/app/template/Index.vm).

The OASIS template data files can be acquired from [Mindboggle](http://mindboggle.info) using this [link](http://mindboggle.info/data/templates/atropos/OASIS-30_Atropos_template.tar.gz) and this [link](http://mindboggle.info/data/atlases/jointfusion/OASIS-TRT-20_jointfusion_DKT31_CMA_labels_in_OASIS-30.nii.gz).

- ndar_run.sge - Bash script to use to submit the ndar_act_cluster.py script in parallel over a cluster of nodes.
- ndar_unpack - Bash-executable Python script which will download and extract imaging data from the NDAR database. Originally cloned from [here](https://raw.githubusercontent.com/chaselgrove/ndar/master/ndar_unpack/ndar_unpack), but slightly modified to add untar-ing functionality.
- ndar_cpac_sublist.py - Script which builds a C-PAC-compatible subject list from an NDAR DB instance. This script can optionally download the S3 imaging data for a local C-PAC run. For this script to work, one must have the following in a csv file so that this script can interact with the AWS cloud-hosted database:

    - Database username
    - Database password
    - Database host URL
    - Database port number
    - Database service id

    This information is given from the NDAR website when you launch a miNDAR instance from the miNDAR tab on NDAR's [cloud page](https://ndar.nih.gov/launch_cloud_db.html). To see a template of the csv file format, see the credentials_template.csv file in this repository. 

    Additionally, the miNDAR database instance must be launched so the user can connect to it (via an internet connection). This is also done on NDAR's [cloud page](https://ndar.nih.gov/launch_cloud_db.html). *Note that the network ports of your internet connection must allow communication through the database port number of the miNDAR instance (aka not firewalled).*

    The rest of the arguments to this function are completely up to the user (see the ndar_cpac_sublist.py docstring for more info).

## Dependencies
Python depenencies:
- [Boto](http://boto.readthedocs.org/en/latest/) - Python package for interacting with Amazon Web Services.
- [cx_Oracle](http://cx-oracle.readthedocs.org/en/latest/index.html) - Python package for interacting with Oracle databases.
- [Nibabel](http://nipy.org/nibabel/api.html) - Python package for read/write access to various neuroimaging data formats.
- [Nipype](http://nipy.sourceforge.net/nipype/documentation.html) - Python package for Neuroimaging data analysis pipelines.
- [Numpy](http://docs.scipy.org/doc/numpy/reference/) - Python package for fast numerical computations.
- [Pandas](http://pandas.pydata.org/) - Python library providing data structures and tools for high-performance data analysis.
- [PyYaml](http://pyyaml.org/wiki/PyYAMLDocumentation) - Python package for parsing and emitting Yaml files.
- [pydicom](https://code.google.com/p/pydicom/) - Python package for working with DICOM files.

Software dependencies:
- [ANTs](http://stnava.github.io/ANTs/) - A popular medical image registration and segmentation toolkit.
- [FreeSurfer](http://surfer.nmr.mgh.harvard.edu/) - An open source software suite for processing and analyzing (human) brain MRI images.
- [FSL](http://fsl.fmrib.ox.ac.uk/fsl/fslwiki/) - A comprehensive library of analysis tools for FMRI, MRI and DTI brain imaging data.
- [minc-tools](https://github.com/BIC-MNI/minc-tools/) - Package of utilities that work on MINC format images.
- [Oracle Instant Client](http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html) - A set of tools to interact with Oracle databases. Only the basic-lite, sdk, and sql packages need to be installed.
- [SQLAlchemy](http://www.sqlalchemy.org/) - Python SQL toolkit and Object Relational Mapper that gives application developers the full power and flexibility of SQL.
