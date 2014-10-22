# act_sublist_build.py
#
# Author: Daniel Clark, 2014

'''
This module contains functions which builds a subject list from a miNDAR
database instance that can then be used to download and process data.
The function takes a range of image03_id's to incorporate when building
the subject list, such that subjects <min_id> through <max_id> (including
both min and max) are used.

Usage:
    python act_sublist_build.py <min_id> <max_id> <creds_path> <out_fp>
'''

# Main routine
def main(min_id, max_id, creds_path, output_path):
    '''
    Method to query the IMAGE03 table from a miNDAR database instance
    and create a subject list of the form (img03_id, s3_path), where
    img03_id is an integer corresponding to the image03_id of the DB
    entry and s3_path is a string corresponding to the path of the
    image on S3. It will save the subject list as a yaml file on disk.

    Parameters
    ----------
    min_id : integer
        The minimum of the image03_id range to build the subject list
    max_id : integer
        The maximum of the image03_id range to build the subject list
    creds_path : string (filepath)
        path to the csv file with 'ACCESS_KEY_ID' as the header and the
        corresponding ASCII text for the key underneath; same with the
        'SECRET_ACCESS_KEY' string and ASCII text
    output_path : string (filepath)
        path to save the output subject list yaml file

    Returns
    -------
    sublist : list (tuple)
        A list of tuples, where each tuple consists of (int, str), 
        corresponding to the image03_id and s3_path of the database
        entry.
    '''
    # Import packages
    import fetch_creds
    import os
    import yaml

    # Init variables
    cursor = fetch_creds.return_cursor(creds_path)
    cmd = '''
          select image03_id, image_file from IMAGE03 
          where 
          image03_id >= :arg_1 and image03_id <= :arg_2
          '''
    out_fp = os.path.abspath(output_path)

    # Execute command
    cursor.execute(cmd, arg_1=min_id, arg_2=max_id)
    res = cursor.fetchall()
    
    # And save result to yaml file
    with open(out_fp, 'w') as f:
        print 'Saving subject list to %s' % out_fp
        f.write(yaml.dump(res))
    f.close()

    # Return the list
    return res


# Run main by default
if __name__ == '__main__':

    # Import packages
    import sys

    # Init variables
    try:
        min_id = int(sys.argv[1])
        max_id = int(sys.argv[2])
        creds_path = str(sys.argv[3])
        output_path = str(sys.argv[4])
    except IndexError as e:
        print 'Not enough input arguments, hit index error: %s' % e
        print __doc__
        sys.exit()

    # Run the main function
    sublist = main(min_id, max_id, creds_path, output_path)

    # Show subject list
    print 'Subject list:'
    print sublist
