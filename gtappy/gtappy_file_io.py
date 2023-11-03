import os
import subprocess
from tkinter import E
import hazelbean as hb
import time
import sys
import pandas as pd
import numpy as np
import math
import harpy


def sl4_to_indexed_dfs(input_har_path, output_index_path):
    """Convert all information in input_har_path into several CSVs that can be programatically rewritten back to a conformant har.
    All paths are written relative to output_index_path (either parallel to or in a newly created output dir)
    
    """
    output_dir = os.path.split(output_index_path)[0]
    label = hb.file_root(output_index_path)
    
    # Will write a separate _index.csv file for fast access of underlying HAR entries, each saved as their own csv.
    har_index_path = output_index_path
    har_sets_path = os.path.join(output_dir, label + '_sets.csv')
    har_csv_dir = os.path.join(output_dir, label)
    hb.create_directories(har_csv_dir)
    
    # Read the harfile using harpy
    InFile = harpy.HarFileObj(filename=input_har_path)

    # Get a list of all headers to iterate through 
    HeadsOnFile = InFile.getHeaderArrayNames()
    
    sl4_object = harpy.SL4(input_har_path)
    print(sl4_object)

    # Define the data recorded to the har_index
    header_data = {
        'header': [],
        'long_name': [],
        'shape': [],
        'dim_names': [],
        'ndims': [],
        'dtype': [],
        'coefficient_name': [],
        }
    
    # Iterate through individual HAR entires
    set_names_dict = {}
    for header in sl4_object.setNames:
        
        # Get a specific header from the InFile
        DataHead = sl4_object.setHeaders[header]
        
        # Draw most values from the actual array (to avoid problems with missing set names etc.)
        shape = DataHead.array.shape
        ndims = len(DataHead.array.shape)
        dtype = DataHead.array.dtype
        
        if 'float' in str(dtype):
            dtype = 'RE'
        
        # Record to the data structure.
        header_data['header'].append(header)
        header_data['long_name'].append(DataHead.long_name)
        
        # Render shape string.
        # in addition to the python types, like shape, there is also the string type for what is written the CSV. This is for pretty rendering but won't load programatically.
        # Maybe I should add a column for python types?
        shape_string = str(shape).replace('(','').replace(')','').replace(' ','')
        if shape_string.endswith(','):
            shape_string = shape_string[:-1]        
        # Kept with HARfile notation that dimensions are split with *
        shape_string = shape_string.replace(',', '*')            
        if shape_string == '':
            shape_string = 1            
        header_data['shape'].append(shape_string)
        
        # IMPORTANT HARFILE NOTE:         
        """Sets are stored on Header Array files as part of real matrices, or individually. In the latter case, the set is stored as an array of strings.
        The difficulty for ViewHAR is to guess (a) which string arrays contain sets, and (b) what are the names of these sets.
        The longname part of the header should be used to record the name of the set, according to the following convention:
        Set IND sectors in the model
        The first word should be Set and the second word should be the name of the set. ViewHAR ignores all words after the first two.
        """
        
        # Extract dim_names. a little harder because not all entries in a HAR file have a value here.           
        if len(DataHead.setNames) > 0:
            if DataHead.setNames[0] and DataHead.setElements[0]: # check that it's not just a list of None
                for c, set_name in enumerate(DataHead.setNames):
                    if set_name in set_names_dict:
                        assert DataHead.setElements[c] == set_names_dict[set_name] # sets with same names have to have same elements.                        
                    elif len(DataHead.setElements) != len(DataHead.setNames):
                        raise NameError('There should be exactly 1 set name for each setElements list.')
                    else:
                        set_names_dict[set_name] = DataHead.setElements[c]
                try:  
                    fstring = str(DataHead.setNames)[1:-1].replace('\'', '').replace(',', '*').replace(' ', '')
                    header_data['dim_names'].append(fstring) # The string manipulation here makes it look nice in excel
                except:
                    header_data['dim_names'].append('')  
            else:
                header_data['dim_names'].append('')  
        else:
            header_data['dim_names'].append('')  
        

        if isinstance(DataHead.setElements, list):   
            if len(DataHead.setNames) > 0:
                if DataHead.setNames[0] is not None:
                    dim_names = [i for i in DataHead.setNames]
                else:
                    dim_names = []
            else:
                dim_names = []
        else:
            if DataHead.setNames is not None:
                dim_names = str(DataHead.setNames).replace(',', '*').replace(' ', '')
            else:
                dim_names = ''
                
        # Finish adding other attributes to the data dict.     
        header_data['ndims'].append(ndims)
        header_data['dtype'].append(dtype)
        header_data['coefficient_name'].append(DataHead.coeff_name)
        
        # Separate from writing the index.csv, we also will write the specific headers csv spreadsheet. This is straightforward
        # for 1 and 2 dimensions, but for three+ we need to stack vertically
        current_header_data_path = os.path.join(har_csv_dir, header + '.csv')
        
        implied_numpy_type = ''
        
            
        
        skip = False
        if len(shape) == 0:
            # row_index = DataHead.setElements[0]
            columns = [header]
            data_array = np.asarray([[DataHead.array]]).T # Pandas requires it to be a 2d array to write, even tho singular
            
            # Test to see if it can be coerced into a float or int
            try:
                nt = np.float32(data_array[[0]])
                implied_numpy_type = 'float32'
            except:
                print ('unable to coerce')
            
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            df_data.to_csv(current_header_data_path, index=False)
        elif len(DataHead.array.shape) == 1:     
            row_index = DataHead.setElements[0]
            columns = [header]   
            data_array = np.asarray([DataHead.array]).T # Pandas requires it to be a 2d array to write, even tho 1dim
            # Test to see if it can be coerced into a float or int
            
            try:
                nt = np.float32(data_array[0, 0])
                
                implied_length = len(data_array[0, 0])
                if implied_length == 12:
                    implied_numpy_type = np.float32
                elif implied_length > 12:
                    implied_numpy_type = np.float64
                else:
                    implied_numpy_type = np.in64
                
            except:
                implied_numpy_type = 'string'
                if isinstance(data_array[0, 0], np.float32):
                    implied_length = 12
                elif isinstance(data_array[0, 0], np.float64):
                    implied_length = 24 # No clue what it actually is for har or if it is even possible.
                else:   
                    implied_length = len(data_array[0, 0])
            
            
            
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            
            dtype_dict = {header: implied_numpy_type}
            
            df_data.to_csv(current_header_data_path, index=False)
        elif len(DataHead.array.shape) == 2:          
            row_index = DataHead.setElements[0]
            columns = DataHead.setElements[1]
            data_array = np.asarray(DataHead.array) # Pandas requires it to be a 2d array to write
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            df_data.to_csv(current_header_data_path)
        elif len(DataHead.array.shape) >= 3:   
            
            ### When going beyond 2 dims, add the leading n - 2 dimensions as a stacked multiindex.    
            # All but the last index will be uses as nested row_indices
            row_indices = [i for i in DataHead.setElements[:-1]]
            
            # The last index will be columns
            columns = DataHead.setElements[-1]            
             
            # Read the raw 3dim array
            data_array = np.asarray(DataHead.array) 
            
            # The number of rows will be equal to the product of the length of all row indices
            n_index_rows = math.prod([len(i) for i in row_indices])
            
            ### Reshape the array to be 2d so that it is n_index_rows tall and n_cols across
            # LEARNING POINT: -1 notation just means "whatever's left over". So when we take a 65*65*141 array and reshape using m * n = 4225, we call .reshape(4225, -1) which would have been equivilent to .reshape(4225, 141) but is more flexible.        
            array_2d = data_array.reshape(n_index_rows, -1)
            
            # Create a pandas multiindex from the product of the two row indices.
            row_multi_index = pd.MultiIndex.from_product(row_indices, names=dim_names[:-1])
                             
            # Create the dataframe
            df_data = pd.DataFrame(index=row_multi_index, columns=columns, data=array_2d)

            df_data.to_csv(current_header_data_path)

    # NOW DO IT AGAIN BUT WITH VARIABLES
    header_data = {
        'header': [],
        'long_name': [],
        'shape': [],
        'dim_names': [],
        'ndims': [],
        'dtype': [],
        'coefficient_name': [],
        }
    
    # Iterate through individual HAR entires
    set_names_dict = {}
    for header, DataHead in sl4_object.variableDict.items():
        
        # Get a specific header from the InFile
        # DataHead = sl4_object.setHeaders[header]
        
        # Draw most values from the actual array (to avoid problems with missing set names etc.)
        shape = DataHead.array.shape
        ndims = len(DataHead.array.shape)
        dtype = DataHead.array.dtype
        
        if 'float' in str(dtype):
            dtype = 'RE'
        
        # Record to the data structure.
        header_data['header'].append(header)
        header_data['long_name'].append(DataHead.long_name)
        
        # Render shape string.
        # in addition to the python types, like shape, there is also the string type for what is written the CSV. This is for pretty rendering but won't load programatically.
        # Maybe I should add a column for python types?
        shape_string = str(shape).replace('(','').replace(')','').replace(' ','')
        if shape_string.endswith(','):
            shape_string = shape_string[:-1]        
        # Kept with HARfile notation that dimensions are split with *
        shape_string = shape_string.replace(',', '*')            
        if shape_string == '':
            shape_string = 1            
        header_data['shape'].append(shape_string)
        
        # IMPORTANT HARFILE NOTE:         
        """Sets are stored on Header Array files as part of real matrices, or individually. In the latter case, the set is stored as an array of strings.
        The difficulty for ViewHAR is to guess (a) which string arrays contain sets, and (b) what are the names of these sets.
        The longname part of the header should be used to record the name of the set, according to the following convention:
        Set IND sectors in the model
        The first word should be Set and the second word should be the name of the set. ViewHAR ignores all words after the first two.
        """
        
        # Extract dim_names. a little harder because not all entries in a HAR file have a value here.           
        if len(DataHead.setNames) > 0:
            if DataHead.setNames[0] and DataHead.setElements[0]: # check that it's not just a list of None
                for c, set_name in enumerate(DataHead.setNames):
                    if set_name in set_names_dict:
                        assert DataHead.setElements[c] == set_names_dict[set_name] # sets with same names have to have same elements.                        
                    elif len(DataHead.setElements) != len(DataHead.setNames):
                        raise NameError('There should be exactly 1 set name for each setElements list.')
                    else:
                        set_names_dict[set_name] = DataHead.setElements[c]
                try:  
                    fstring = str(DataHead.setNames)[1:-1].replace('\'', '').replace(',', '*').replace(' ', '')
                    header_data['dim_names'].append(fstring) # The string manipulation here makes it look nice in excel
                except:
                    header_data['dim_names'].append('')  
            else:
                header_data['dim_names'].append('')  
        else:
            header_data['dim_names'].append('')  
        

        if isinstance(DataHead.setElements, list):   
            if len(DataHead.setNames) > 0:
                if DataHead.setNames[0] is not None:
                    dim_names = [i for i in DataHead.setNames]
                else:
                    dim_names = []
            else:
                dim_names = []
        else:
            if DataHead.setNames is not None:
                dim_names = str(DataHead.setNames).replace(',', '*').replace(' ', '')
            else:
                dim_names = ''
                
        # Finish adding other attributes to the data dict.     
        header_data['ndims'].append(ndims)
        header_data['dtype'].append(dtype)
        header_data['coefficient_name'].append(DataHead.coeff_name)
        
        # Separate from writing the index.csv, we also will write the specific headers csv spreadsheet. This is straightforward
        # for 1 and 2 dimensions, but for three+ we need to stack vertically
        current_header_data_path = os.path.join(har_csv_dir, header + '.csv')
        
        implied_numpy_type = ''
        
            
        if header == 'qgdp':
            print ('here')
        skip = False
        if len(shape) == 0:
            # row_index = DataHead.setElements[0]
            row_index= [1]
            columns = [header]
            data_array = np.asarray([[DataHead.array]]).T # Pandas requires it to be a 2d array to write, even tho singular
            
            # Test to see if it can be coerced into a float or int
            try:
                nt = np.float32(data_array[[0]])
                implied_numpy_type = 'float32'
            except:
                print ('unable to coerce')
            
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            df_data.to_csv(current_header_data_path, index=False)
        elif len(DataHead.array.shape) == 1:     
            row_index = DataHead.setElements[0]
            columns = [header]   
            data_array = np.asarray([DataHead.array]).T # Pandas requires it to be a 2d array to write, even tho 1dim
            # Test to see if it can be coerced into a float or int
            
            try:
                nt = np.float32(data_array[0, 0])
                
                implied_length = len(data_array[0, 0])
                if implied_length == 12:
                    implied_numpy_type = np.float32
                elif implied_length > 12:
                    implied_numpy_type = np.float64
                else:
                    implied_numpy_type = np.in64
                
            except:
                implied_numpy_type = 'string'
                if isinstance(data_array[0, 0], np.float32):
                    implied_length = 12
                elif isinstance(data_array[0, 0], np.float64):
                    implied_length = 24 # No clue what it actually is for har or if it is even possible.
                else:   
                    implied_length = len(data_array[0, 0])
            
            
            
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            
            dtype_dict = {header: implied_numpy_type}
            
            df_data.to_csv(current_header_data_path, index=True) # NOTE EXCEPTION TO WHEN I USUALLY DROP INDICES.
        elif len(DataHead.array.shape) == 2:          
            row_index = DataHead.setElements[0]
            columns = DataHead.setElements[1]
            data_array = np.asarray(DataHead.array) # Pandas requires it to be a 2d array to write
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            df_data.to_csv(current_header_data_path)
        elif len(DataHead.array.shape) >= 3:   
            
            ### When going beyond 2 dims, add the leading n - 2 dimensions as a stacked multiindex.    
            # All but the last index will be uses as nested row_indices
            row_indices = [i for i in DataHead.setElements[:-1]]
            
            # The last index will be columns
            columns = DataHead.setElements[-1]            
             
            # Read the raw 3dim array
            data_array = np.asarray(DataHead.array) 
            
            # The number of rows will be equal to the product of the length of all row indices
            n_index_rows = math.prod([len(i) for i in row_indices])
            
            ### Reshape the array to be 2d so that it is n_index_rows tall and n_cols across
            # LEARNING POINT: -1 notation just means "whatever's left over". So when we take a 65*65*141 array and reshape using m * n = 4225, we call .reshape(4225, -1) which would have been equivilent to .reshape(4225, 141) but is more flexible.        
            array_2d = data_array.reshape(n_index_rows, -1)
            
            # Create a pandas multiindex from the product of the two row indices.
            row_multi_index = pd.MultiIndex.from_product(row_indices, names=dim_names[:-1])
                             
            # Create the dataframe
            df_data = pd.DataFrame(index=row_multi_index, columns=columns, data=array_2d)

            df_data.to_csv(current_header_data_path)
    
    # Only the base data seems to be distributed with Sets.har files. sl4s do not have this. Thus only run if there is something that populates set_names_dict
    if len(set_names_dict) > 0:
        for set_name, set_elements in set_names_dict.items():
            print('Adding header for set: ', set_name, set_elements)


            header_data['header'].append(set_name)
            header_data['long_name'].append('Set ' + set_name) # NOTE: This is literally defined by ViewHAR and is used in TABLO that the first word set means the second word set_name is a set. Subsequent words are ignored.
            header_data['shape'].append(len(set_elements))
            header_data['dim_names'].append(set_name)
            header_data['ndims'].append(1)
            header_data['dtype'].append('<U12')
            header_data['coefficient_name'].append('')
        
            columns = [set_name]   
            data_array = np.asarray(set_elements).T # Pandas requires it to be a 2d array to write, even tho 1dim
            # Test to see if it can be coerced into a float or int
            
            # try:
            #     nt = np.float32(data_array[0, 0])
                
            #     implied_length = len(data_array[0, 0])
            #     if implied_length == 12:
            #         implied_numpy_type = np.float32
            #     elif implied_length > 12:
            #         implied_numpy_type = np.float64
            #     else:
            #         implied_numpy_type = np.in64
                
            # except:
            #     implied_numpy_type = 'string'
            #     if isinstance(data_array[0, 0], np.float32):
            #         implied_length = 12
            #     elif isinstance(data_array[0, 0], np.float64):
            #         implied_length = 24 # No clue what it actually is for har or if it is even possible.
            #     else:   
            #         implied_length = len(data_array[0, 0])
            
            
            # df_data = pd.DataFrame(index=row_multi_index, columns=columns, data=array_2d)
            
            current_header_data_path = os.path.join(har_csv_dir, set_name + '.csv')
            df_data = pd.DataFrame(columns=columns, data=data_array)
            df_data.to_csv(current_header_data_path, index=False)
            
    df_index = pd.DataFrame(data=header_data)
    df_index.to_csv(har_index_path, index=False)
        
        # hb.python_object_to_csv(set_names_dict, har_sets_path, csv_type='2d_odict_list')
        # df_sets = pd.DataFrame(data=set_names_dict) # Can't export with pandas cause different lengths



def har_to_indexed_dfs(input_har_path, output_index_path):
    """Convert all information in input_har_path into several CSVs that can be programatically rewritten back to a conformant har.
    All paths are written relative to output_index_path (either parallel to or in a newly created output dir)
    
    """
    output_dir = os.path.split(output_index_path)[0]
    label = hb.file_root(output_index_path)
    
    # Will write a separate _index.csv file for fast access of underlying HAR entries, each saved as their own csv.
    har_index_path = output_index_path
    har_sets_path = os.path.join(output_dir, label + '_sets.csv')
    har_csv_dir = os.path.join(output_dir, label)
    hb.create_directories(har_csv_dir)
    
    # Read the harfile using harpy
    InFile = harpy.HarFileObj(filename=input_har_path)

    # Get a list of all headers to iterate through 
    HeadsOnFile = InFile.getHeaderArrayNames()
    
    # Define the data recorded to the har_index
    header_data = {
        'header': [],
        'long_name': [],
        'shape': [],
        'dim_names': [],
        'ndims': [],
        'dtype': [],
        'coefficient_name': [],
        }
    
    # Iterate through individual HAR entires
    set_names_dict = {}
    for header in HeadsOnFile:
        
        # START HERE: consider something like if header in headers_to_skip: Currently it is failing because DREL is a 1C which messes up the RE data type logic.
        # Get a specific header from the InFile
        DataHead = InFile[header]
        
        # Draw most values from the actual array (to avoid problems with missing set names etc.)
        shape = DataHead.array.shape
        ndims = len(DataHead.array.shape)
        dtype = DataHead.array.dtype
        
        if 'float' in str(dtype):
            dtype = 'RE'
        
        # Record to the data structure.
        header_data['header'].append(header)
        header_data['long_name'].append(DataHead.long_name)
        
        # Render shape string.
        # in addition to the python types, like shape, there is also the string type for what is written the CSV. This is for pretty rendering but won't load programatically.
        # Maybe I should add a column for python types?
        shape_string = str(shape).replace('(','').replace(')','').replace(' ','')
        if shape_string.endswith(','):
            shape_string = shape_string[:-1]        
        # Kept with HARfile notation that dimensions are split with *
        shape_string = shape_string.replace(',', '*')            
        if shape_string == '':
            shape_string = 1            
        header_data['shape'].append(shape_string)
        
        # IMPORTANT HARFILE NOTE:         
        """Sets are stored on Header Array files as part of real matrices, or individually. In the latter case, the set is stored as an array of strings.
        The difficulty for ViewHAR is to guess (a) which string arrays contain sets, and (b) what are the names of these sets.
        The longname part of the header should be used to record the name of the set, according to the following convention:
        Set IND sectors in the model
        The first word should be Set and the second word should be the name of the set. ViewHAR ignores all words after the first two.
        """
        
        # Extract dim_names. a little harder because not all entries in a HAR file have a value here.           
        if len(DataHead.setNames) > 0:
            if DataHead.setNames[0] and DataHead.setElements[0]: # check that it's not just a list of None
                for c, set_name in enumerate(DataHead.setNames):
                    if set_name in set_names_dict:
                        assert DataHead.setElements[c] == set_names_dict[set_name] # sets with same names have to have same elements.                        
                    elif len(DataHead.setElements) != len(DataHead.setNames):
                        raise NameError('There should be exactly 1 set name for each setElements list.')
                    else:
                        set_names_dict[set_name] = DataHead.setElements[c]
                try:  
                    fstring = str(DataHead.setNames)[1:-1].replace('\'', '').replace(',', '*').replace(' ', '')
                    header_data['dim_names'].append(fstring) # The string manipulation here makes it look nice in excel
                except:
                    header_data['dim_names'].append('')  
            else:
                header_data['dim_names'].append('')  
        else:
            header_data['dim_names'].append('')  
        

        if isinstance(DataHead.setElements, list):   
            if len(DataHead.setNames) > 0:
                if DataHead.setNames[0] is not None:
                    dim_names = [i for i in DataHead.setNames]
                else:
                    dim_names = []
            else:
                dim_names = []
        else:
            if DataHead.setNames is not None:
                dim_names = str(DataHead.setNames).replace(',', '*').replace(' ', '')
            else:
                dim_names = ''
                
        # Finish adding other attributes to the data dict.     
        header_data['ndims'].append(ndims)
        header_data['dtype'].append(dtype)
        header_data['coefficient_name'].append(DataHead.coeff_name)
        
        # Separate from writing the index.csv, we also will write the specific headers csv spreadsheet. This is straightforward
        # for 1 and 2 dimensions, but for three+ we need to stack vertically
        current_header_data_path = os.path.join(har_csv_dir, header + '.csv')
        
        implied_numpy_type = ''
        
            
        
        skip = False
        if len(shape) == 0:
            # row_index = DataHead.setElements[0]
            columns = [header]
            data_array = np.asarray([[DataHead.array]]).T # Pandas requires it to be a 2d array to write, even tho singular
            
            # Test to see if it can be coerced into a float or int
            try:
                nt = np.float32(data_array[[0]])
                implied_numpy_type = 'float32'
            except:
                print ('unable to coerce')
            
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            df_data.to_csv(current_header_data_path, index=False)
        elif len(DataHead.array.shape) == 1:     
            row_index = DataHead.setElements[0]
            columns = [header]   
            data_array = np.asarray([DataHead.array]).T # Pandas requires it to be a 2d array to write, even tho 1dim
            # Test to see if it can be coerced into a float or int
            
            try:
                nt = np.float32(data_array[0, 0])
                
                implied_length = len(data_array[0, 0])
                if implied_length == 12:
                    implied_numpy_type = np.float32
                elif implied_length > 12:
                    implied_numpy_type = np.float64
                else:
                    implied_numpy_type = np.in64
                
            except:
                implied_numpy_type = 'string'
                if isinstance(data_array[0, 0], np.float32):
                    implied_length = 12
                elif isinstance(data_array[0, 0], np.float64):
                    implied_length = 24 # No clue what it actually is for har or if it is even possible.
                else:   
                    implied_length = len(data_array[0, 0])
            
            
            
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            
            dtype_dict = {header: implied_numpy_type}
            
            df_data.to_csv(current_header_data_path, index=False)
        elif len(DataHead.array.shape) == 2:          
            row_index = DataHead.setElements[0]
            columns = DataHead.setElements[1]
            data_array = np.asarray(DataHead.array) # Pandas requires it to be a 2d array to write
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            df_data.to_csv(current_header_data_path)
        elif len(DataHead.array.shape) >= 3:   
            
            ### When going beyond 2 dims, add the leading n - 2 dimensions as a stacked multiindex.    
            # All but the last index will be uses as nested row_indices
            row_indices = [i for i in DataHead.setElements[:-1]]
            
            # The last index will be columns
            columns = DataHead.setElements[-1]            
             
            # Read the raw 3dim array
            data_array = np.asarray(DataHead.array) 
            
            # The number of rows will be equal to the product of the length of all row indices
            n_index_rows = math.prod([len(i) for i in row_indices])
            
            ### Reshape the array to be 2d so that it is n_index_rows tall and n_cols across
            # LEARNING POINT: -1 notation just means "whatever's left over". So when we take a 65*65*141 array and reshape using m * n = 4225, we call .reshape(4225, -1) which would have been equivilent to .reshape(4225, 141) but is more flexible.        
            array_2d = data_array.reshape(n_index_rows, -1)
            
            # Create a pandas multiindex from the product of the two row indices.
            row_multi_index = pd.MultiIndex.from_product(row_indices, names=dim_names[:-1])
                             
            # Create the dataframe
            df_data = pd.DataFrame(index=row_multi_index, columns=columns, data=array_2d)

            df_data.to_csv(current_header_data_path)


    
    # Only the base data seems to be distributed with Sets.har files. sl4s do not have this. Thus only run if there is something that populates set_names_dict
    if len(set_names_dict) > 0:
        for set_name, set_elements in set_names_dict.items():
            print('Adding header for set: ', set_name, set_elements)


            header_data['header'].append(set_name)
            header_data['long_name'].append('Set ' + set_name) # NOTE: This is literally defined by ViewHAR and is used in TABLO that the first word set means the second word set_name is a set. Subsequent words are ignored.
            header_data['shape'].append(len(set_elements))
            header_data['dim_names'].append(set_name)
            header_data['ndims'].append(1)
            header_data['dtype'].append('<U12')
            header_data['coefficient_name'].append('')
        
            columns = [set_name]   
            data_array = np.asarray(set_elements).T # Pandas requires it to be a 2d array to write, even tho 1dim
            # Test to see if it can be coerced into a float or int
            
            # try:
            #     nt = np.float32(data_array[0, 0])
                
            #     implied_length = len(data_array[0, 0])
            #     if implied_length == 12:
            #         implied_numpy_type = np.float32
            #     elif implied_length > 12:
            #         implied_numpy_type = np.float64
            #     else:
            #         implied_numpy_type = np.in64
                
            # except:
            #     implied_numpy_type = 'string'
            #     if isinstance(data_array[0, 0], np.float32):
            #         implied_length = 12
            #     elif isinstance(data_array[0, 0], np.float64):
            #         implied_length = 24 # No clue what it actually is for har or if it is even possible.
            #     else:   
            #         implied_length = len(data_array[0, 0])
            
            
            # df_data = pd.DataFrame(index=row_multi_index, columns=columns, data=array_2d)
            
            current_header_data_path = os.path.join(har_csv_dir, set_name + '.csv')
            df_data = pd.DataFrame(columns=columns, data=data_array)
            df_data.to_csv(current_header_data_path, index=False)
            
    df_index = pd.DataFrame(data=header_data)
    df_index.to_csv(har_index_path, index=False)
        
        # hb.python_object_to_csv(set_names_dict, har_sets_path, csv_type='2d_odict_list')
        # df_sets = pd.DataFrame(data=set_names_dict) # Can't export with pandas cause different lengths


def indexed_dfs_to_har(input_indexed_dfs_path, output_har_path, verbose=False):

    
    index_df_dir, index_df_filename = os.path.split(input_indexed_dfs_path)
    index_name = os.path.splitext(index_df_filename)[0]
    index_df = pd.read_csv(input_indexed_dfs_path, index_col=0)
    
    # Prior to writing ARRAYS, we need to load the relevant sets to get the actually n-dim array shape.
    # So iterate through the headers looking for things that follow the ViewHAR convention of the long_name being Set NAME   
    sets_data = {} 
    for header in index_df.index:
        index_df_row = index_df.loc[index_df.index == header]
        long_name = index_df_row['long_name'].values[0]
        split_name = long_name.split(' ')
        if len(split_name) >= 2:
            annotation = split_name[0]
            possible_set_label = split_name[1]
            if annotation == 'Set':
                # print('Found set', possible_set_label)
                set_data = pd.read_csv(os.path.join(index_df_dir, index_name, possible_set_label + '.csv'))
                sets_data[possible_set_label] = list(set_data.values)
    # Create a new Harfile object.
    Harfile = harpy.HarFileObj(filename=output_har_path)
    
    # Based on the headers listed in the index CSV, add new headers to the Harfile.

    
    # I got messed up because of longer-than-four header names, which HAR saves instead as
    # a coefficient name with a numerical header id. Still
    # working to reverse engineer this. Consider having a backwards 
    # look-up table to convert from 0001 to psave or whatever.
    # This is still not perfect because it doesn't preserve the numerical header indices of the input.
    numerical_header_counter = 1
    long_to_short_header_dict = {}
    short_to_long_header_dict = {}
    
    for header in index_df.index:
        skip_write = False
        data_df_path = os.path.join(index_df_dir, index_name, header + '.csv')
        if len(header) > 4:
            long_header = header
            short_header = str(numerical_header_counter).rjust(4, '0')
            long_to_short_header_dict[long_header] = short_header
            short_to_long_header_dict[short_header] = long_header
            numerical_header_counter += 1

            header = long_header

        else:
            long_header = header
            short_header = header

        if hb.path_exists(data_df_path): # Note that HAR Files are hard-coded to assume this is 4. However, some dimension labels in GTAP are more than 4, like TARTYPE, so you cant infer between each other. 
            # Load columns of index_df for use in writing.
            index_df_row = index_df.loc[index_df.index == header]
            long_name = index_df_row['long_name'].values[0]
            shape = index_df_row['shape'].values[0]
            dtype = index_df_row['dtype'].values[0]
            coefficient_name = index_df_row['coefficient_name'].values[0]  
            if not isinstance(coefficient_name, str):
                coefficient_name = ''
            dim_names = list(index_df_row['dim_names'].values)            
            
            # Given the name of the header as loaded from the index, open the CSV file that contains that header's data.
            
            data_df = pd.read_csv(data_df_path)
            
            if len(data_df) == 0:
                skip_write = True
            set_elements = []           
            if len(dim_names) > 0:
                
                # Some headers don't have dim names which pandas will interpret as nans. Check for that.
                try: 
                    tried_is_nan = math.isnan(dim_names[0])
                except: 
                    tried_is_nan = False
                    
                # If it's not nan, can read it directly
                if not tried_is_nan:

                    # To keep with the look-and-feel of ViewHAR, dimensions are stored split with a *
                    dim_names_as_list = dim_names[0].split('*')
                    for dim_name in dim_names_as_list:
                        cur_col = sets_data[dim_name]
                        cur_values = [i[0] for i in cur_col]
                        set_elements.append(cur_values)
                        # as_list = list(sets_df[dim_name].dropna()) # NOTE Because this was loaded from non-rectangular DF, need to manually trim the excess rows once we know what column it is.
                        # set_elements.append(as_list)
                    zipped =  dict(zip(dim_names_as_list, set_elements)) # BUG!!! Dicts cannot store duplicate strings in their keys. Thus it drops the second REG in COMM*REG*REG. # Fixed this by not getting the shapes from the zipped, but instead directly from the elements list.               
                    implied_shape = [len(i) for i in set_elements]
                    
                    # Because indices past dim1 are recorded as columns, need to add them on to the before the reshape and then drop
                    # FUTURE feature: I Think I could have just used pandas multi-indexes more smartly to avoid having to store dimensions as data.                
                    if len(implied_shape) >= 2:
                        n_indices_stored_in_rows = len(implied_shape) - 1
                    elif len(implied_shape) == 1 and implied_shape[0] > 1:
                        n_indices_stored_in_rows = 0
                    else:
                        n_indices_stored_in_rows = 0

                    # Correct for the case where it is a named var but only 1 dim but still has the dim labels
                    try:
                        test_len =  len(data_df.values[0])
                        if test_len == 2:
                            n_indices_stored_in_rows = 1
                    except:
                        'nah'
                    # Select the non-dimension data
                    unshaped = data_df.values 
                    unshaped = unshaped[:, n_indices_stored_in_rows:]
                    
                    # Reshape it to the shape implied by the list of elements.
                    array_entries = unshaped.reshape(implied_shape)

                # If it is nan, just store a blank value for now.
                else:
                    dim_names_as_list = ['']
                    if header in data_df:
                        array_entries = data_df[header].values
                        set_elements = None
                        dim_names = ['']
                        zipped = None
                    else:
                        try:
                            array_entries = data_df['0'].values
                            set_elements = None
                            dim_names = ['']
                            zipped = None     
                            skip_write = True        
                        except:
                            array_entries = np.float32(0)
                            set_elements = None
                            dim_names = ['']
                            zipped = None       
                            
                            skip_write = True                 

            else:
                raise NameError('wtf')   
                     
            
            if verbose:
                print ('indexed_dfs_to_har is writing ' + str(header), dtype)
                
            # Create a new Header object from the values loaded from the DFs. 
            har_dtype = dtype
            if not skip_write:
                Header = harpy.HeaderArrayObj.HeaderArrayFromData(array_entries, coefficient_name, long_name, dim_names_as_list, zipped, har_dtype)
                    
                # Add it to the Harfile object
                Harfile[short_header] = Header
        
    # Once all headers have been added, write it to disk.
    Harfile.writeToDisk()


def directory_of_hars_to_indexed_dfs(input_dir, output_dir=None, produce_hars_from_csvs=None, verbose=True):
    
    if output_dir is None:
        output_dir = input_dir
    hb.create_directories(output_dir)
    
    hars_to_look_for = hb.list_filtered_paths_nonrecursively(input_dir, include_extensions='.har')
    
    for har_filename in hars_to_look_for:
        if verbose:
            print('Extracting ' + str(har_filename))
            
        # Write har to CSVs
        har_index_path = os.path.join(output_dir, hb.file_root(har_filename) + '.csv')     
         
        if hb.path_exists(har_filename):
            hb.create_directories(output_dir)
            
            if not hb.path_exists(har_index_path, verbose=True): # Minor note, could add more robust file validation to check for ALL the implied files to exist.
            
                # Extract the har to the indexed DF format.
                har_to_indexed_dfs(har_filename, har_index_path)       
                
            if produce_hars_from_csvs:

                # For validation (and actual use in the model), create a new har from the indexed dir.
                validation_har_path = hb.path_replace_extension(har_index_path, '.har')
                indexed_dfs_to_har(har_index_path, validation_har_path)
    

def get_set_labels_from_index_path(input_path):
    # The Indexed CSV format adds set information to the bottom of the inxex file. These are identified as 
    # sets if their long_name starts with 'Set '
    # Use this structure to extract a list of sets present in the indexed CSV.
    df = pd.read_csv(input_path)
    df_sets = df.loc[df.long_name.str.startswith('Set ')]
    set_labels = list(df_sets['header'])
    
    return set_labels