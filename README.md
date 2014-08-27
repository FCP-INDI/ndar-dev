This repository contains the code used to preprocess data from the NDAR database and upload the results/log files to tables in the database.

## Repository files
- act_interface.py - Nipype interface made to work with the ANTs cortical thickness extraction script found [here](https://raw.githubusercontent.com/stnava/ANTs/master/Scripts/antsCorticalThickness.sh)
- ndar_act_cluster.py - Streamlined script to execute the nipype workflow from the interface defined in act_interface.py and then upload the results and log files to the NDAR database. This script is designed to be launched from a cluster of C-PAC AMI's on AWS using the Sun Grid Engine job scheduler.
*Note: The `fetch_creds` module needed for this database is a custom-module which returns AWS and RDS Oracle DB sensitive information. One can replace the variables this module returns with their own keys and database log-in information to use this functionality.*

- ndar_run.sge - Bash script to use to submit the ndar_act_cluster.py script in parallel over a cluster of nodes.
- ndar_unpack - Bash-executable Python script which will download and extract imaging data from the NDAR database. Originally cloned from [here](https://raw.githubusercontent.com/chaselgrove/ndar/master/ndar_unpack/ndar_unpack), but slightly modified to add untar-ing functionality.
- aws_walkthrough.txt - Instructions on how to use AWS EC2 to launch and interact with a C-PAC AMI.

The OASIS template data files can be acquired from [Mindboggle](http://mindboggle.info) using this [link](http://mindboggle.info/data/templates/atropos/OASIS-30_Atropos_template.tar.gz) and this [link](http://mindboggle.info/data/atlases/jointfusion/OASIS-TRT-20_jointfusion_DKT31_CMA_labels_in_OASIS-30.nii.gz).

## Dependencies
Python depenencies:
- [Boto](http://boto.readthedocs.org/en/latest/) - Python package for interacting with Amazon Web Services.
- [cx_Oracle](http://cx-oracle.readthedocs.org/en/latest/index.html) - Python package for interacting with Oracle databases.
- [Nibabel](http://nipy.org/nibabel/api.html) - Python package for read/write access to various neuroimaging data formats.
- [Nipype](http://nipy.sourceforge.net/nipype/documentation.html) - Python package for Neuroimaging data analysis pipelines.
- [Numpy](http://docs.scipy.org/doc/numpy/reference/) - Python package for fast numerical computations.
- [PyYaml](http://pyyaml.org/wiki/PyYAMLDocumentation) - Python package for parsing and emitting Yaml files.
- [pydicom](https://code.google.com/p/pydicom/) - Python package for working with DICOM files.

Software dependencies:
- [ANTs](http://stnava.github.io/ANTs/) - A popular medical image registration and segmentation toolkit.
- [FreeSurfer](http://surfer.nmr.mgh.harvard.edu/) - An open source software suite for processing and analyzing (human) brain MRI images.
- [FSL](http://fsl.fmrib.ox.ac.uk/fsl/fslwiki/) - A comprehensive library of analysis tools for FMRI, MRI and DTI brain imaging data.
- [Oracle Instant Client](http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html) - A set of tools to interact with Oracle databases. Only the basic-lite, sdk, and sql packages need to be installed.
