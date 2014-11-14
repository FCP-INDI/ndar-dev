# insert_abide_results.py
#
# Author: Daniel Clark, 2014

'''
This tool contains functions which populate the abide_img_results
table on the miNDAR database using ABIDE preprocessed data stored in
Amazon's S3 service.

Usage: <requred_input>, [optional_input]
    python insert_abide_results.py -bc <bucket_creds_path>
                                   -dc <db_creds_path>
                                   -b <bucket_name>
                                   -bp [bucket_prefix]
                                   -nr [number_expected_results]
''' 


# Main routine
def main(creds_path, creds_path2, bucket, b_prefix, pipeline, num_res):
    '''
    Function that analyzes data in an S3 bucket and then uploads it
    into a tabular format as an entry in a database table

    Parameters
    ----------
    creds_path : string
        filepath to the S3 bucket credentials as a csv file
    creds_path2 : string
        filepath to the database instance credentials as a csv file
    bucket : string
        name of the S3 bucket to analyze data from
    b_prefix : string
        prefix filepath within the S3 bucket to parse for data
    pipeline : string
        name of the pipeline to gather outputs from for tabulating in DB
    num_res : integer
        the number of results you would expect the pipeline to have per
        derivative when checking if the information was already entered

    Returns
    src_list : list (boto Keys)
        a list of the keys that were inserted into the database
    '''

    # Import packages
    import fetch_creds
    # ANTs
    if pipeline == 'ants':
        import ants_insert as db_insert
    # CIVET
    elif pipeline == 'civet':
        import civet_insert as db_insert
    # Freesurfer
    elif pipeline == 'freesurfer':
        import freesurfer_insert as db_insert
    # Otherwise, assume its ccs, cpac, dparsf, or niak
    else:
        import insert_utils as db_insert

    # Init variables
    prefix = 'https://s3.amazonaws.com/' + bucket

    # Get AWS keys
    b = fetch_creds.return_bucket(creds_path, bucket)
    cursor = fetch_creds.return_cursor(creds_path2)

    # Set up lists of keys
    src_list = b.list(prefix=b_prefix)
    file_list = [s for s in src_list if pipeline in str(s.name)]

    # Part of the list is already uploaded, hack off some
    no_files = len(file_list)
    print 'done creating file list, it has %d elements' % no_files

    # Iterate through list
    i = 0
    for f in file_list:
        url_path = prefix + str(f.name)
        exists = check_existing(cursor, url_path, 'abide_img_results', num_res)
        if not exists:
            db_insert.upload_results(cursor, url_path)
            print 'uploaded file %s successfully!' % url_path
        else:
            print 'already loaded file %s, skipping...' % url_path
        i += 1
        per = 100*(float(i)/no_files)
        print 'done with file %d/%d\n%f%% complete\n' % \
        (i, no_files, per)

    # Return the src_list
    return src_list


# Run main by default
if __name__ == '__main__':

    # Import packages
    import argparse
    import os

    # Init argparser
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-bc', '--bucket_creds', nargs=1, required=True,
                        help='Credentials to access the S3 bucket items')
    parser.add_argument('-dc', '--db_creds', nargs=1, required=True,
                        help='Credentals to access the database instance tables')
    parser.add_argument('-b', '--bucket', nargs=1, required=True,
                        help='S3 bucket name')
    parser.add_argument('-bp', '--bucket_prefix', nargs=1, required=False,
                        help='Base folders prefix of S3 bucket to search')
    parser.add_argument('-nr', '--num_res', nargs=1, required=False,
                        help='Number of expected results to be in table if '\
                             'the entry(ies) might be there already')

    args = parser.parse_args()

    # Init varables
    creds_path = os.path.abspath(args.bucket_creds[0])
    creds_path2 = os.path.abspath(args.db_creds[0])
    bucket = str(args.bucket[0])
    if args.bucket_prefix:
        b_prefix = str(args.bucket_prefix[0])
    else:
        b_prefix = None
    if args.num_res:
        num_res = int(args.num_res[0])
    else:
        num_res = 1

    # Call main with input args
    main(creds_path, creds_path2, bucket, b_prefix, num_res)
