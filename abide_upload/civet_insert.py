# civet_insert.py
#
# Author: Daniel Clark, 2014

'''
This module contains functions which populate the abide_img_results
table on the miNDAR database using ABIDE preprocessed data stored in
Amazon's S3 service.
'''


# Go through dat file and create roi dictionary
def get_rois_from_dat(roi_list, first=2, last=10, split_str=None):
    '''
    Method to extract the roi key-value pairs from a list of
    strings and return them as a dictionary

    Parameters
    ----------
    roi_list : list (str)
        list of strings where the ROI label and ROI value are
        separated by a tab character and begin at list element first
        and end at list element last
    first : integer, default = 2
        integer of roi_list such that roi_list[first] returns the
        first occurence of an roi label-value pair
    last : integer, default = 10
        integer of roi_list such that roi_list[first] returns the
        last occurence of an roi label-value pair
    split_str : string, default = None
        string to use to split the data by

    Returns
    -------
    roi_dict : dictionary
        A dictionary with the ROI label as the key and the ROI
        float as the value
    '''

    # Init variables
    roi_dict = {}

    # Check length of roi_list to determine what roi_descriptions to use
    if last-first == 8:
        roi_desc = ['midsaggittal, brainstem, corpus callosum',
                    'parietal lobe',
                    'occipital lobe',
                    'frontal lobe',
                    'posterior cingulate',
                    'parahippocampal gyrus',
                    'cingulate gyrus minus posterior',
                    'temporal lobe minus parahippocampal gyrus',
                    'insula']
    elif last-first == 2:
        # If label/values are split by a :, its a gi file
        if split_str == ':':
            roi_desc = ['gyrification index of gray matter',
                        'gyrification index of white matter',
                        'gyrification index of mid']
        else:
            roi_desc = ['CSF','gray matter','white matter']
    else:
        roi_desc = {i:'' for i in range(len(roi_list))}

    #print 'roi_list', roi_list
    #print 'roi_desc', roi_desc
    # Iterate through list
    i = 0
    for l in range(first, last+1):
        #   print 'split_str', split_str
        keyval = roi_list[l].split(split_str)
        #print 'keyval', keyval
        roi_dict[keyval[0]] = [float(keyval[1]),roi_desc[i]]
        i += 1

    # Return dictionary of roi key-value pairs
    return roi_dict


# Select which kind of dat file info to use
def fetch_civet_info(url_path):
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
    # Template
    if 'native' in url_path:
        template = 'native'
    else:
        template = 'MNI ICBM152'

    # Check if it's a dat file
    if url_path.endswith('.dat'):
        # If it has 'cerebral_volume' in the title, init field values and extract ROIs
        if 'cerebral_volume' in url_path:
            # Set indices for ROI extraction
            beg = 0
            end = 2
            # Set name field
            name = 'volume of cortex in native space'
            # Set measurename field
            measure = 'volume'
            template = 'native'
            units = 'mm^3'

        # If it has 'gi_' in the title, init field values and extract ROIs
        elif 'gi_' in url_path:
            # Set indices for ROI extraction
            beg = 0
            end = 2
            # Set name field
            name = 'gyrification index'
            # Set measurename field
            measure = 'volume'
            template = 'native'
            units = 'ratio'
            split_str = ':'

        # If it has 'lobe_areas' in the title, init field values and extract ROIs
        elif 'lobe_areas' in url_path:
            # Set indices for ROI extraction
            beg = 2
            end = 10
            # Set name field
            name = 'vertex-based cortical elementary area'
            measure = 'regional average'
            units = 'mm^2'

        # If it has 'native_cortex_area' in the title, init field values and extract ROIs
        elif 'native_cortex_area' in url_path:
            # Set indices for ROI extraction
            beg = 2
            end = 10
            # Set name field
            name = 'cortical area in native space'
            # Set measurename field
            measure = 'regional average'
            units = 'mm^2'

        # If it has 'thickness_tlink' in the title, init field values and extract ROIs
        elif 'thickness_tlink' in url_path:
            # Set indices for ROI extraction
            beg = 2
            end = 10
            # Set name field
            name = 'cortical thickness'
            # Set measurename field
            measure = 'regional average'
            units = 'mm'

        # If it has 'lobe_volumes' in the title, init field values and extract ROIs
        elif 'lobe_volumes' in url_path:
            # Set indices for ROI extraction
            beg = 2
            end = 10
            # Set name field
            name = 'vertex-based volumes'
            # Set measurename field
            measure = 'regional average'
            units = 'mm^3'

        # Otherwise, it's not a dat file, skip it
        else:
            raise ValueError, 'unexpected input %s, check url_path' % url_path

    # Otherwise, it's either a .obj or .txt file
    else:
        # .obj files
        # If it has 'gray_surface_rsl' in the title, init field values
        if 'gray_surface_rsl' in url_path:
            # Set name field
            name = 'resampled gray matter surfaces'
            # Set measurename field
            measure = 'model surfaces'
            units = ''
        # If it has 'mid_surface_rsl' in the title, init field values
        elif 'mid_surface_rsl' in url_path:
            # Set name field
            name = 'resampled mid surfaces (between white and gray)'
            # Set measurename field
            measure = 'model surfaces'
            units = ''
        # If it has 'white_surface_rsl' in the title, init field values
        elif 'white_surface_rsl' in url_path:
            # Set name field
            name = 'resampled white matter surfaces'
            # Set measurename field
            measure = 'model surfaces'
            units = ''
        # .txt files
        # If it has 'pos_rsl_asym_hemi' in the title, init field values
        elif 'pos_rsl_asym_hemi' in url_path:
            # Set name field
            name = 'assymetry map for position on mid resampled surfaces by hemisphere'
            # Set measurename field
            measure = 'position map'
            units = 'mm'
        # If it has 'rms_rsl_tlink_30mm_asym_hemi' in the title, init field values
        elif 'rms_rsl_tlink_30mm_asym_hemi' in url_path:
            # Set name field
            name = 'asymmetry cortical thickness map (resampled left-right)'
            # Set measurename field
            measure = 'thickness map'
            units = 'mm'
        # If it has 'rms_rsl_tlink' in the title, init field values
        elif 'rms_rsl_tlink' in url_path:
            # Set name field
            name = 'cortical thickness'
            # Set measurename field
            measure = 'thickness map'
            units = 'mm'
        # If it has 'native_volume' in the title, init field values
        elif 'native_volume' in url_path:
            # Set name field
            name = 'vertex-based elementary volumes on resampled hemispheric surfaces'
            # Set measurename field
            measure = 'volume map'
            units = 'mm^3'
        # Otherwise, it's not a dat file, skip it
        else:
            raise ValueError, 'unexpected input %s, check url_path' % url_path

    # Return the dat-specific info
    return beg, end, name, measure, template, units, split_str


# Insert lobe ROI values and total from civet
def upload_results(cursor, url_path):
    '''
    Method to insert CIVET pipeline data from .dat, .obj, .txt files
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
    import fetch_creds
    import time
    import urllib

    # Init variables
    #url_file = urllib.urlopen(url_path)
    #file_contents = url_file.readlines()
    # Known field values for CIVET pipeline results
    deriv_id = return_next_pk(cursor)
    # Pipeline info
    pname = 'CIVET'
    ptype = 'Executable C, Perl'
    ptools = 'C, C++, Perl'
    pver = '2.0.0'
    pdesc = 'Corticometry Analysis Tools'
    # S3 path
    s3_path = url_path
    # Get datasetid and guid
    fname = url_path.split('/')[-1]
    sub_id = find_subid(fname)
    datasetid, guid = return_datasetid_guid(cursor,sub_id)
    # Unknowns
    cfg_file_loc = ''
    strategy = 'tricubic interpolation registration, t1only brain masking'
    atlas = 'BIC visual QC atlas'
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

    # Get blurring kernel if it was applied
    fwhm = ''
    if 'mm' in url_path:
        url_split = url_path.split('_')
        mm = [s for s in url_split if 'mm' in s]
        try:
            fwhm = mm[0].split('mm')[0]
        except IndexError as e:
            print 'Error in getting the blurring kernel fwhm\n', e
    # And set kernel extension to add to name column of entry
    if fwhm:
        kernel_ext = ' using %s mm fwhm blurring kernel' % fwhm
    else:
        kernel_ext = ''

    # Get the specific file's info
    print 'Analyzing %s...' % url_path
    beg, end, name, measure, template, units, split_str = fetch_civet_info(url_path)
    name = name + kernel_ext

    # If it's a dat file, get the specific file's info
    if url_path.endswith('.dat'):
        # Extract ROI values and upload
        roi_dict = get_rois_from_dat(file_contents, first=beg, last=end, 
                                     split_str=split_str)
        print 'Found %d ROIs, inserting them into table...' % len(roi_dict)

        # Now iterate through the ROI dictionary and insert the data
        for roi,val in roi_dict.items():
            # Timestamp
            timestamp = str(time.ctime(time.time()))
            # ROI description
            value = val[0]
            roidesc = val[1]
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

    # If it's a txt file, get the specific file's info
    if url_path.endswith('.txt'):
        # Timestamp
        timestamp = str(time.ctime(time.time()))
        # ROI description
        value = ''
        roi = ''
        roidesc = ''
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
