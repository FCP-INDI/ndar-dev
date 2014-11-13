AWS EC2 Walkthrough
-------------------
1. Go to http://aws.amazon.com/console/
2. Click the "Sign in to the AWS Console" button
3. Enter your credentials: Account, User Name, Password
4. Click on "EC2" - Amazon's Elastic Compute Cloud service. This allows you to configure and launch virtual machines in Amazon's cloud
5. You will see the EC2 Management Console. Here you can configure and launch new virtual machines, or instances. These instances are launched from a saved-state virtual machine known as an "AMI", or Amazon Machine Image.
6. Amazon has different regions that it hosts its web services from, e.g. Oregon, Northern Virginia, Tokyo, etc. In the upper right-hand corner there will be a region that you are logged into next to your user name. Change this to N. Virginia (as that is where all of the development work up to this point has been done). * Note if you are not in the N. Virginia region, you will not be able to see the CPAC AMI when you search the community AMIs.
7. In the left-hand column under the "INSTANCES" header, click "Instances". This is a dashboard of all instances you currently have running on the cloud in AWS. Click the blue "Launch Instance" button.
8. On the left-hand side of the new page, click on the "Community AMIs" tab and search "cpac" in the search text box.
9. The "C-PAC w/benchmark" AMI should appear. Choose the AMI and hit "Select."
10. This next page is where you choose the instance type, that is, the hardware specifications for your instance. Typically, for CPAC to run effectively, around 16GB (or greater) of RAM is optimal. The m3.xlarge instance type has 15GB of RAM and 4 CPUs and functions well with CPAC. To select this type, click on the "General purpose" tab and select the m3.xlarge size instance and click the "Next: Configure Instance Details" button.
11. This page can be used to launch multiple instances from this AMI, or request Spot instances as well as other things (including virtual private network, VPC options, etc). For now, we do not need to do anything here, but it can be customized in the future. Click "Next: Add Storage."
12. Here we can change how much storage is allocated for the instance being launched. For the CPAC Benchmark, about 125GB is suficient. Click "Next: Tag Instance."
13. Here, we can tag the instance to give it a name as a reminder for why we launched it. Something like 'CPAC-Demo' works. Click "Next: Configure Security Group."
14. Here is where we can modify who has access to this instance. The launch-wizard security group being created with the instance works (as it allows for ssh access). If you would like to customize security and user access to the AMI, it can be done here. Click "Review and Launch."
15. This final page summarizes the instance details you are about to launch. Everything should look ok (there might be some warnings at the top as a result of security or instance type being not in the free tier, which is ok).
16. Click the "Launch" button. A dialogue box opens asking about choosing a key pair for the instance. Every instance requires a key pair in order to securely login and use it. If you have not created a key pair yet, change the top drop down menu to "Create a new key pair." Then name the key pair something like "user-cpacdemo-northva". Click "Download Key Pair" and save it to your machine.
17. Change the top drop down menu bar to "Choose an existing key pair" and select the name of the key pair you just downloaded in the other drop down menu. Check the acknowledgement check box and launch the instance.
18. You can click the "View Instances" blue button on the lower right of the page after to watch the instance start up.
19. Once it is up and running (should say 2/2 under "Status checks" column), you can ssh into the instance and use it. Click on the instance and copy the string of the instance "Public DNS" (...amazonaws.com or something)
20. Open a terminal and type: `ssh -i {path/to/keypair.pem} -X ubuntu@{public-dns-ofinstasnce-amazonaws.com}`
21. It will start the connection and ask if you trust the source; type "yes"
22. You should now be in the instance! There should be cpac related files in /home/ubuntu/. Feel free to launch `cpac_gui` and have at it!

AWS EC2 Cluster configuration
-----------------------------
1. The C-PAC AMI can be used in a cluster configuration to improve and allow for parallel computation performance.
2. A cluster of instances can be managed and configured through AWS directly, however the process is tedious and requires knowledge on how to set up a cluster of machines (with things like NFS, passwordless ssh, SGE/PBS/etc job scheduling, security groups, master node and slave nodes, etc.).
3. A utility called [Starcluster](http://star.mit.edu/cluster/) can be used to automate this configuration and streamline the entire process to get things up and running more quickly. There is great user documentation on the website on how to set up and configure Starcluster to work with you AWS credentials.
4. You can use this software to configure any AMI in a cluster set up, however, to do so for the C-PAC AMI, use AMI ID ami-cc74e1a4 in the starcluster config file.

Benchmark Walkthrough
---------------------
1. Follow the above AWS EC2 walkthrough above and select the "C-PAC with benchmark" AMI in step 9. Follow through to step 22 to launch the C-PAC GUI once logged in to the instance.
2. On the left-hand side of the main window, click the "Load" button under "Subject Lists."
3. Select the benchmark subject list yaml file at "/home/ubuntu/cpac_benchmark/settings/subject_lists/CPAC_subject_list.yml"
4. Name the subject list (e.g. "Benchmark Subjects") and click "OK"
5. Check the checkbox next to the subject list name. You can also click "View" here to parse through the subject list to see if it looks correct.
6. On the right-hand side of the main window, click the "Load" button under "Pipelines."
7. Select the pipeline configuration yaml file at "/home/ubuntu/cpac_benchmark/settings/configs/pipeline_config_benchmark.yml"
8. Check the checkbox next to the pipeline configuration name.
9. You can click "Edit" if you wish to change any of the default parameters of the pipeline configuration. Here you can choose what types of preprocessing to do and which outputs to produce.
10. Once you are done editing the pipeline configuration, save it.
11. Back at the main window, the CPAC subject list and the CPAC pipeline config checkboxes should both be checked.
12. Now click "Run Individual Level Analysis" at the bottom of the window and the run will start.
13. It is possible that you may run in to errors while processing. Please check our [Github page](https://github.com/FCP-INDI/C-PAC) and look through the Issues list for support. Feel free to post new issues here as well.
14. At the conclusion of the run, all of the outputs produced should be in "/home/ubuntu/cpac_benchmark/output".

miNDAR CPAC Walkthrough
---------------------
1. Follow the above AWS EC2 walkthrough above and select the "C-PAC w/benchmark" AMI in step 9. Follow through to step 21 to log in to the instance.
2. Now a subject list needs to be built from the miNDAR database of interest. First, ensure that the minDAR database instance is launched from the [NDAR website](http://ndar.nih.gov/).
3. The database instance will have the following parameters that are needed to connect to it:
    - Database username
    - Database password
    - Database host URL
    - Database port number
    - Database service id
4. Insert these values into a csv file in the format of the `credentials_template.csv` file that is included in this repository; save this csv file to a directory on your computer.
5. Run `ndar_cpac_sublist.py <images_base_dir> <study_name> <creds_path> <output_sublist_path>`, where:
    - `<images_base_dir>` is a local base folder on your computer where all of the downloaded images will  be stored and organized for input into CPAC, e.g. /path/to/inputs
    - `<study_name>` is the name of the study that the data is associated with; this is just a string that the user can specify for convenience, and the images will be saved into this directory under <images_base_dir>, e.g. study_abc
    - `<creds_path>` is the full path to the csv credentials file created in step 4, e.g. /path/to/creds.csv
    - `<output_sublist_path>` is the full path to a yaml file on disk, e.g. /path/to/sublist.yml
   This function should then query and download the images from the miNDAR database instance to the local computer, and extract them into a hierarchy that CPAC is compatible with, all under the <images_base_dir> directory. It will finally write the subject list to disk at <output_sublist_path>.
6. Once this is done running successfully, open the CPAC GUI with: cpac_gui
7. On the left-hand side of the main window, click the "Load" button under "Subject Lists."
8. Select the created subject list yaml file at <output_sublist_path>.
9. Name the subject list (e.g. "miNDAR Subjects") and click "OK"
10. Check the checkbox next to the subject list name. You can also click "View" here to parse through the subject list to see if it looks correct.
11. On the right-hand side of the main window, click the "Load" button under "Pipelines."
12. To get a starting point, the benchmark pipeline configuration file can be used. Select the pipeline configuration yaml file at "/home/ubuntu/cpac_benchmark/settings/configs/pipeline_config_benchmark.yml"
13. Check the checkbox next to the pipeline configuration name.
14. Click the "Edit" button to begin configuring the pipeline. Under "Computer Settings" are options to select the output, working, and crash directories. The defaults will work, but are pointed to the benchmark folder. These can be changed to different directories if one wishes. Furthermore, the different derivatives of interest can be set and parameterized here.
15. Once you are done editing the pipeline configuration, save it.
11. Back at the main window, the CPAC subject list and the CPAC pipeline config checkboxes should both be checked.
12. Now click "Run Individual Level Analysis" at the bottom of the window and the run will start.
13. It is possible that you may run in to errors while processing. Please check our [Github page](https://github.com/FCP-INDI/C-PAC) and look through the Issues list for support. Feel free to post new issues here as well.
14. At the conclusion of the run, all of the outputs produced should be in specified output directory chosen under the "Computer Settings" section of the pipeline configuration.
15. Now that all of the desired data is written to the output, uploading the results to an AWS S3 bucket or miNDAR database table is possible. Since this process is a very user-specific task, where each user may have different miNDAR table names, table columns, etc. and different S3 bucket names and key paths, C-PAC does not attempt to upload these results itself. However there are examples of how this is done for the ANTS cortical thickness results in the `ndar_act_run.py` script that is included in this repository.
