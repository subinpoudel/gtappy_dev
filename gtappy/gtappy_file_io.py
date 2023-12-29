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
import warnings

def ndstack_indexed_csv(input_file_path, output_file_path, headers_to_stack='all', headers_to_ignore='default'):
    """Reads an nd_index csv file and converts it to a nd_stacked csv file.
    The nd_stacked_csv file is a very flexible format that combines data of many different
    dimensions, set-definintions, and naming schemes while preserving all of the information.
    It does this by defining 1-7 pairs of dimN_label and dimN_value columns, where N is the dimension number.
    The file size for this is huge (Though it compresses well), but it is very flexible for doing
    pivot tables.
    
    Writes to output_file_path.
    
    returns the stacked dataframe.
    
    """
    
    
    # Read the index
    df_index = pd.read_csv(input_file_path)
    if headers_to_stack == 'all':
        headers_to_stack = df_index['header'].tolist()
        
    if headers_to_ignore == 'default':
        headers_to_ignore = ['XXNP']
        
    # Remove headers to ignore from the headers_to_stack list
    headers_to_stack = [i for i in headers_to_stack if i not in headers_to_ignore] 
    
    # Get only the subset of the index that will be stacked
    df_index = df_index[df_index['header'].isin(headers_to_stack)]
    
    # Create a new df to merge with the data
    df = df_index.copy()    

    # ADD new label-value columns for each dimension
    total_n_dims = 7 # Because Hars are defined this way with a max of this dimensions.
    for c, dim in enumerate(range(total_n_dims + 1)):
        dim_label = 'dim' + str(c) + '_label'
        dim_value = 'dim' + str(c) + '_value'
        
        # Add dim_label as a new empty column in df
        df[dim_label] = ''
        df[dim_value] = ''
        
    # Iterate through headers, saving individual DFs into this list for concatenation later
    dfs_to_stack = []
    for header_c, header in enumerate([i.lower() for i in headers_to_stack]):

        # Analyze row in index for current header

        current_row = df[df['header'].str.lower() == header]

        hb.log(str(header_c/len(headers_to_stack) * 100), 'percent done. Converting header ', header, 'from', input_file_path, 'to', output_file_path, level=20)      
        
        # The logic below is CONVOLUTED AS HELL! But it works. It's just a lot of edge cases to consider.
        current_dims_pre = str(current_row['dim_names'].values[0])
        current_shape_pre = current_row['shape'].values[0]
        is_singular = False
        if current_dims_pre == 'nan' and current_shape_pre == str(1):
            is_singular = True
        try:
            a = list(current_row['dim_names'].iloc[0])
            it_broke = False
        except:
            it_broke = True
        if it_broke:
            current_dims = ['value']
        elif '*' in current_row['dim_names'].iloc[0]:
            current_dims = current_row['dim_names'].iloc[0].split('*')
        else:
            current_dims = [current_row['dim_names'].iloc[0]]
        
        for c, dim in enumerate(current_dims):
            dim_label_col = 'dim' + str(c) + '_label'
            dim_value_col = 'dim' + str(c) + '_value'
            
            current_row.loc[:, dim_label_col] = dim
                        
        header_data_dirname = os.path.splitext(input_file_path)[0]
        header_data_dir = os.path.join(os.path.split(input_file_path)[0], header_data_dirname)
        header_data_path = os.path.join(header_data_dir, header + '.csv')
        
        data_df = pd.read_csv(header_data_path)
        
        # LEARNING POINT: using
        # data_df = pd.read_csv(header_data_path, index_col=0)
        # causes us to LOSE a column of data! wtf       

        data_df = data_df.rename(columns={data_df.columns[0]: current_dims[0]})
        
        hb.log('BEFORE set index\n\n', data_df, level=100)
        # Set the index to be this column
        if len(current_dims) == 1:
            if len(data_df) > 1:
                if len(data_df.columns) == 1:
                    pass
                    # data_df = data_df.set_index(current_dims[0]
                else:
                    data_df = data_df.set_index(current_dims[0])
            else:
                if is_singular:
                    pass
                elif len(data_df.columns) == 1:
                    pass
                    # data_df = data_df.set_index(current_dims[0])
                else:
                    data_df = data_df.set_index(current_dims[0])
        else:
            data_df = data_df.set_index(current_dims[0:-1])      
        hb.log('AFTER set index\n\n', data_df, level=100)
        
        if len(current_dims) > 1:
            hb.log('columns before creating multiindex', list(data_df.columns), level=100)
            # Create a multiindex from dimensions and values
            data_df.columns = pd.MultiIndex.from_product([current_dims[-1:], data_df.columns], names=[current_dims[-1] + '_index', current_dims[-1]])

            hb.log('columns after creating multiindex', data_df.columns, level=100)
            hb.log(data_df, level=100)
            
            data_df_stacked = data_df.stack()        
        else:
            if is_singular:
                data_df_stacked = data_df
            else:
                data_df_stacked = data_df
            
        hb.log('Stacked\n\n', data_df_stacked, level=100)
        
        # Flatten things out so it can go in the n-dim stacked output
        data_df_stacked.columns = ['value']        
        data_df_stacked['header'] = header        
        hb.log('added header for merging with index\n', data_df_stacked, level=100)
        
        # reset the index so they are preserved when merged, but first check for duplicate index names        
        duplicated_cols = hb.list_find_duplicates(data_df_stacked.index.names)
        if len(duplicated_cols) > 0:
            hb.log('DUPLICATES FOUND IN INDEX. Renaming.', duplicated_cols, level=100)
            data_df_stacked = data_df_stacked.reset_index(allow_duplicates=True)
            columns_list = data_df_stacked.columns.tolist()
            for c, column in enumerate(columns_list):
                if column in duplicated_cols:
                    columns_list[c] = column + '_' + str(c)
                    current_dims[c] = column + '_' + str(c)
            data_df_stacked.columns = columns_list
        data_df_stacked = data_df_stacked.reset_index() # allow_duplicates=True) resulted in two columns being returned later on        
        hb.log('data_df_stacked_after_reset_index\n', data_df_stacked, level=100)
        
        # Note that we only merge with the selection because we will concatenate at the end for speed
        df_selection = df[df['header'] == header]
        df_current = hb.df_merge(df_selection, data_df_stacked, on='header', how='outer', verbose=False)
        
        hb.log('df after merge with stacked \n', df, level=100)
        for c, dim in enumerate(current_dims):
            dim_label_col = 'dim' + str(c) + '_label'
            dim_value_col = 'dim' + str(c) + '_value'
            
            df_current[dim_label_col] = dim
            
            if dim in df_current.columns:
                df_current[dim_value_col] = df_current[dim]
            else:
                df_current[dim_value_col] = df_current['value']
            
            if dim != 'value':
                if dim in df_current.columns:
                    df_current = df_current.drop(columns=[dim])
            
        # Now do the actual value
        dim_label_col = 'dim' + str(c+1) + '_label'
        dim_value_col = 'dim' + str(c+1) + '_value'   

        df_current[dim_label_col] = 'value'
        try:        
            df_current[dim_value_col] = df_current['value'].astype(float)          
        except:
            df_current[dim_value_col] = df_current['value']
            
        df_current = df_current.drop(columns=['value'])
        if 'index' in df_current.columns:
            df_current = df_current.drop(columns=['index'])
        dfs_to_stack.append(df_current)
        
    # Stack everything in df_to_stack
    df = pd.concat(dfs_to_stack)
    
    # Change the dtype of 'value' column to float in-place
    # But note that because in the stacked format we don't know from the column labels which
    # is the actual column that stores 'value', we get it from the last assigned value of 
    # dim_value_col
    
    # first test to see if it's floatable
    try:
        df[dim_value_col] = df[dim_value_col].astype(float)
    except:
        pass # Must have been string.
    
    df.to_csv(hb.suri(output_file_path, ''), index=False)
    
    return df
 
    
def read_ndstacked_csv(input_file_path):
    # Small function to read nd_stacked csvs withotu throwing the error that
    # there are different dtypes ina column (which is actually intended because of the nd-stacked structure)
    from pandas.errors import DtypeWarning
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DtypeWarning)
        df = pd.read_csv(input_file_path)
    return df

def sl4_to_ndindexed_dfs(input_har_path, output_index_path):
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
    set_names = sl4_object.setNames

    for header_c, header in enumerate(sl4_object.setNames):
        if header.lower() == 'reg':
            pass
        # hb.log(str(header_c/len(sl4_object.setNames) * 100), 'percent done. Converting header ', header, 'from', input_har_path, 'to', output_index_path, level=20)
        
        # Get a specific header from the InFile
        DataHead = sl4_object.setHeaders[header]
        
        # Draw most values from the actual array (to avoid problems with missing set names etc.)
        shape = DataHead.array.shape
        ndims = len(DataHead.array.shape)
        dtype = DataHead.array.dtype
        
        if 'float' in str(dtype):
            dtype = 'RE'
        
        # Record to the data structure.
        header_data['header'].append(header.strip())
        header_data['long_name'].append(DataHead.long_name.strip())
        
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
                        assert [i.strip() for i in DataHead.setElements[c]] == set_names_dict[set_name] # sets with same names have to have same elements.                        
                    elif len(DataHead.setElements) != len(DataHead.setNames):
                        raise NameError('There should be exactly 1 set name for each setElements list.')
                    else:
                        set_names_dict[set_name] = [i.strip() for i in DataHead.setElements[c]]
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
                    dim_names = [i.strip() for i in DataHead.setNames]
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
        header_data['coefficient_name'].append(str(DataHead.coeff_name).strip())
        
        # Separate from writing the index.csv, we also will write the specific headers csv spreadsheet. This is straightforward
        # for 1 and 2 dimensions, but for three+ we need to stack vertically
        current_header_data_path = os.path.join(har_csv_dir, header + '.csv')
        
        implied_numpy_type = ''
        
        if header.lower() == 'reg':
            pass
            # print(data_array)        
             
        
        skip = False
        if len(shape) == 0:
            # row_index = DataHead.setElements[0]
            columns = [header.strip()]
            data_array = np.asarray([[DataHead.array]]).T # Pandas requires it to be a 2d array to write, even tho singular
            
            # See if it's a string in there
            try:
                implied_numpy_type = 'string'
                data_array = data_array.astype(str)  # Convert to string type if all elements can be safely converted
                data_array = np.char.strip(data_array)                  
                if isinstance(data_array[[0]], str):
                    implied_numpy_type = 'string'
                    data_array = data_array.astype(str)  # Convert to string type if all elements can be safely converted
                    data_array = np.char.strip(data_array)                    
            except:
                pass
                    
                
            # Test to see if it can be coerced into a float or int
            try:
                nt = np.float32(data_array[[0]])
                implied_numpy_type = 'float32'
            except:
                print ('unable to coerce')
            hb.log('not sure if i could get here', row_index)
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            df_data.to_csv(current_header_data_path, index=False)
        elif len(DataHead.array.shape) == 1:     
            row_index = [i.strip() for i in DataHead.setElements[0]]
            columns = [header.strip()]   
            data_array = np.asarray([DataHead.array]).T # Pandas requires it to be a 2d array to write, even tho 1dim
            
            # See if it's a string in there
            try:
                implied_numpy_type = 'string'
                data_array = data_array.astype(str)  # Convert to string type if all elements can be safely converted
                data_array = np.char.strip(data_array)                     
                if isinstance(data_array[[0]], str) or data_array[[0]].dtype is dtype('<U12'):
                    implied_numpy_type = 'string'
                    data_array = data_array.astype(str)  # Convert to string type if all elements can be safely converted
                    data_array = np.char.strip(data_array)                    
            except:
                pass            
            
            # Test to see if it can be coerced into a float or int
            
            try:
                nt = np.float32(data_array[0, 0])
                
                implied_length = len(data_array[0, 0])
                if implied_length == 12:
                    implied_numpy_type = np.float32
                elif implied_length > 12:
                    implied_numpy_type = np.float64
                else:
                    implied_numpy_type = np.int64
                
            except:
                implied_numpy_type = 'string'
                try:
                    if isinstance(data_array[0, 0], np.float32):
                        implied_length = 12
                    elif isinstance(data_array[0, 0], np.float64):
                        implied_length = 24 # No clue what it actually is for har or if it is even possible.
                    else:   
                        implied_length = len(data_array[0, 0])
                except:
                    implied_length = 12
            
            if header.lower() == 'reg':
                pass
                print(data_array)
            
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            if header.lower() == 'reg':
                pass
                print(df_data)            
                
                
            dtype_dict = {header: implied_numpy_type}
            
            df_data.to_csv(current_header_data_path, index=False)
        elif len(DataHead.array.shape) == 2:          
            row_index = [i.strip() for i in DataHead.setElements[0]]
            columns = [i.strip() for i in DataHead.setElements[1]]
            data_array = np.asarray(DataHead.array) # Pandas requires it to be a 2d array to write
            
            # See if it's a string in there
            try:
                implied_numpy_type = 'string'
                data_array = data_array.astype(str)  # Convert to string type if all elements can be safely converted
                data_array = np.char.strip(data_array)                  
                if isinstance(data_array[[0]], str):
                    implied_numpy_type = 'string'
                    data_array = data_array.astype(str)  # Convert to string type if all elements can be safely converted
                    data_array = np.char.strip(data_array)                    
            except:
                pass
            
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            df_data.to_csv(current_header_data_path)
        elif len(DataHead.array.shape) >= 3:   
            
            ### When going beyond 2 dims, add the leading n - 2 dimensions as a stacked multiindex.    
            # All but the last index will be uses as nested row_indices
            row_indices = [i.strip() for i in DataHead.setElements[:-1]]
            
            # The last index will be columns
             
            columns = DataHead.setElements[-1].strip()            
            # columns = [i.strip() for i in DataHead.setElements[-1]]            
            # Read the raw 3dim array
            data_array = np.asarray(DataHead.array) 
            
            # The number of rows will be equal to the product of the length of all row indices
            n_index_rows = math.prod([len(i) for i in row_indices])
            
            ### Reshape the array to be 2d so that it is n_index_rows tall and n_cols across
            # LEARNING POINT: -1 notation just means "whatever's left over". So when we take a 65*65*141 array and reshape using m * n = 4225, we call .reshape(4225, -1) which would have been equivilent to .reshape(4225, 141) but is more flexible.        
            array_2d = data_array.reshape(n_index_rows, -1)
            
            # See if it's a string in there
            try:
                if isinstance(array_2d[[0]], str):
                    implied_numpy_type = 'string'
                    array_2d = array_2d.astype(str)  # Convert to string type if all elements can be safely converted
                    array_2d = np.char.strip(array_2d)                    
            except:
                pass            
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
        print('header', header)
        if header.lower() == 'pds':
            pass
            print(data_array)        
            
        # Get a specific header from the InFile
        # DataHead = sl4_object.setHeaders[header]
        
        # Draw most values from the actual array (to avoid problems with missing set names etc.)
        shape = DataHead.array.shape
        ndims = len(DataHead.array.shape)
        dtype = DataHead.array.dtype
        
        if 'float' in str(dtype):
            dtype = 'RE'
        
        # Record to the data structure.
        header_data['header'].append(header.strip())
        header_data['long_name'].append(DataHead.long_name.strip())
        
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
                        assert [i.strip() for i in DataHead.setElements[c]] == set_names_dict[set_name] # sets with same names have to have same elements.                        
                    elif len(DataHead.setElements) != len(DataHead.setNames):
                        raise NameError('There should be exactly 1 set name for each setElements list.')
                    else:
                        set_names_dict[set_name] = [i.strip() for i in DataHead.setElements[c]]
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
                    dim_names = [i.strip() for i in DataHead.setNames]
                else:
                    dim_names = []
            else:
                dim_names = []
        else:
            if DataHead.setNames is not None:
                dim_names = str(DataHead.setNames).replace(',', '*').replace(' ', '')
            else:
                dim_names = ''
        
        if ndims == 0:
            ndims = 1
            
        # Finish adding other attributes to the data dict.     
        header_data['ndims'].append(ndims)
        header_data['dtype'].append(dtype)
        header_data['coefficient_name'].append(str(DataHead.coeff_name).strip())
        
        # Separate from writing the index.csv, we also will write the specific headers csv spreadsheet. This is straightforward
        # for 1 and 2 dimensions, but for three+ we need to stack vertically
        current_header_data_path = os.path.join(har_csv_dir, header + '.csv')
        
        implied_numpy_type = ''

        skip = False
        if len(shape) == 0:
            # row_index = DataHead.setElements[0]
            row_index = [1]
            columns = [header.strip() ]
            data_array = np.asarray([[DataHead.array]]).T # Pandas requires it to be a 2d array to write, even tho singular
            data_array = data_array.astype(str)  # Convert to string type if all elements can be safely converted
            data_array = np.char.strip(data_array)     
            # Test to see if it can be coerced into a float or int
            try:
                nt = np.float32(data_array[[0]])
                implied_numpy_type = 'float32'
            except:
                print ('unable to coerce')
            
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            df_data.to_csv(current_header_data_path, index=False)
        elif len(DataHead.array.shape) == 1:     
            row_index = [i.strip() for i in DataHead.setElements[0]]
            columns = [header.strip() ]
            # columns = [[i.strip() for i in header]]   
            data_array = np.asarray([DataHead.array]).T # Pandas requires it to be a 2d array to write, even tho 1dim
            # Test to see if it can be coerced into a float or int
            data_array = data_array.astype(str)  # Convert to string type if all elements can be safely converted
            data_array = np.char.strip(data_array)   
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
            row_index = [i.strip() for i in DataHead.setElements[0]]
            columns = [i.strip() for i in DataHead.setElements[1]]
            data_array = np.asarray(DataHead.array) # Pandas requires it to be a 2d array to write
            data_array = data_array.astype(str)  # Convert to string type if all elements can be safely converted
            data_array = np.char.strip(data_array)               
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            df_data.to_csv(current_header_data_path)
        elif len(DataHead.array.shape) >= 3:   
            
            ### When going beyond 2 dims, add the leading n - 2 dimensions as a stacked multiindex.    
            # All but the last index will be uses as nested row_indices
            row_indices = [[j.strip() for j in i]for i in DataHead.setElements[:-1]]
            
            # The last index will be columns
            columns = [i.strip() for i in DataHead.setElements[-1]]          
             
            # Read the raw 3dim array
            data_array = np.asarray(DataHead.array) 
            data_array = data_array.astype(str)  # Convert to string type if all elements can be safely converted
            data_array = np.char.strip(data_array)               
            
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


            header_data['header'].append(set_name.strip())
            header_data['long_name'].append('Set ' + set_name.strip()) # NOTE: This is literally defined by ViewHAR and is used in TABLO that the first word set means the second word set_name is a set. Subsequent words are ignored.
            header_data['shape'].append(len([i.strip() for i in set_elements]))
            header_data['dim_names'].append(set_name.strip())
            header_data['ndims'].append(1)
            header_data['dtype'].append('<U12')
            header_data['coefficient_name'].append('')
        
            columns = [set_name]   
            data_array = np.asarray(set_elements).T
            
            data_array = data_array.astype(str)  # Convert to string type if all elements can be safely converted
            data_array = np.char.strip(data_array)  
            
            
            # Pandas requires it to be a 2d array to write, even tho 1dim
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

        if header.lower() == 'pds':
            pass



    df_index = pd.DataFrame(data=header_data)
    df_index.to_csv(har_index_path, index=False)
        
        # hb.python_object_to_csv(set_names_dict, har_sets_path, csv_type='2d_odict_list')
        # df_sets = pd.DataFrame(data=set_names_dict) # Can't export with pandas cause different lengths



def har_to_ndindexed_dfs(input_har_path, output_index_path):
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
    # headers_to_iterate = [i for i in HeadsOnFile if i ]
    for header in HeadsOnFile:
        
        # Get a specific header from the InFile
        DataHead = InFile[header]
        
        # Draw most values from the actual array (to avoid problems with missing set names etc.)
        shape = DataHead.array.shape
        if str(shape) == '()': # For the DVER variable, it was a singleton integer, which numpy i think regards as 1 dim but other contexts it's 0-dim. Overwrite that there
            shape = (1,)
        ndims = len(shape)
        dtype = DataHead.array.dtype        

        if 'float' in str(dtype):
            dtype = 'RE'
        
        # Record to the data structure.
        header_data['header'].append(str(header).lstrip().rstrip())
        header_data['long_name'].append(str(DataHead.long_name).lstrip().rstrip())
        
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
                    trimmed_set_name = set_name.replace(' ', '')
                    if trimmed_set_name in set_names_dict:
                        pass
                        a = [i.replace(' ', '') for i in DataHead.setElements[c]]
                        b = set_names_dict[set_name]
                        if a != b:
                            print ('WARNING SETS WITH SAME NAME HAVE DIFFERENT ELEMENTS')
                            print (a)
                            print (b)
                        # assert DataHead.setElements[c] == set_names_dict[set_name] # sets with same names have to have same elements.                        
                    elif len(DataHead.setElements) != len(DataHead.setNames):
                        raise NameError('There should be exactly 1 set name for each setElements list.')
                    else:
                        a = DataHead.setElements[c]
                        set_names_dict[trimmed_set_name] = a
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
        header_data['coefficient_name'].append(str(DataHead.coeff_name).lstrip().rstrip())
        
        # Separate from writing the index.csv, we also will write the specific headers csv spreadsheet. This is straightforward
        # for 1 and 2 dimensions, but for three+ we need to stack vertically
        current_header_data_path = os.path.join(har_csv_dir, header + '.csv')
        
        implied_numpy_type = ''

        if len(shape) == 0:
            try:
                row_index = DataHead.setElements[0]
            except:
                row_index = [1]
            columns = [header]
            data_array = np.asarray([[DataHead.array]]).T # Pandas requires it to be a 2d array to write, even tho singular
            dim_names
            # Test to see if it can be coerced into a float or int
            try:
                nt = np.float32(data_array[[0]])
                implied_numpy_type = 'float32'
            except:
                print ('unable to coerce')
            
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            df_data.to_csv(current_header_data_path, index=False)
        elif len(DataHead.array.shape) == 1 or len(shape) == 1:  
            try:
                row_index = DataHead.setElements[0]
            except:
                row_index = [1]
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
                if len(data_array.shape) == 1:
                    if isinstance(data_array[0], np.float32):
                        implied_length = 12
                    elif isinstance(data_array[0], np.float64):
                        implied_length = 24 # No clue what it actually is for har or if it is even possible.
                    else:   
                        implied_length = len(data_array[0])
                else:
                    if isinstance(data_array[0, 0], np.float32):
                        implied_length = 12
                    elif isinstance(data_array[0, 0], np.float64):
                        implied_length = 24 # No clue what it actually is for har or if it is even possible.
                    else:   
                        implied_length = len(data_array[0, 0])
                
                
            
            df_data = pd.DataFrame(index=row_index, columns=columns, data=data_array)
            
            if row_index and len(dim_names) > 0:
                df_data = df_data.reset_index()
                df_data = df_data.rename(columns={'index': dim_names[0]})
                
            dtype_dict = {header: implied_numpy_type}
            
            # START HERE, set to false if is set, otherwise not
            
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
            # print ('Adding header for set: ', set_name, set_elements)

            if set_name in header_data['header']:
                # print ('set_name', set_name, 'already in header_data. Skipping.')
                skip_adding_header = True
            else:
                skip_adding_header = False
                
            if not skip_adding_header:
                
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
                # print ('Found set', possible_set_label)
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
            if short_header in Harfile:
                skip_write = True
                hb.log('Skipping writing header', short_header)
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
            print ('Extracting ' + str(har_filename))
            
        # Write har to CSVs
        har_index_path = os.path.join(output_dir, hb.file_root(har_filename) + '.csv')     
         
        if hb.path_exists(har_filename):
            hb.create_directories(output_dir)
            
            if not hb.path_exists(har_index_path, verbose=True): # Minor note, could add more robust file validation to check for ALL the implied files to exist.
            
                # Extract the har to the indexed DF format.
                har_to_ndindexed_dfs(har_filename, har_index_path)       
                
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

def assert_two_indexed_csv_paths_are_identical(left_path, right_path, approximate_ok=False):
    if approximate_ok:
        df1 = pd.read_csv(left_path)
        df2 = pd.read_csv(right_path)
    
        if not str(df1.head()) == str(df2.head()):
            print ('df1.head()')
            print (df1.head())  
            print ('df2.head()')
            print (df2.head())
            raise Exception('df1.head() != df2.head()')

        if not str(df1.tail()) == str(df2.tail()):
            print ('df1.tail()')
            print (df1.tail())
            print ('df2.tail()')
            print (df2.tail())
            raise Exception('df1.tail() != df2.tail()')

        df1_dtypes = df1.dtypes
        df2_dtypes = df2.dtypes
        if not str(df1_dtypes) == str(df2_dtypes):
            print ('df1_dtypes')
            print (df1_dtypes)
            print ('df2_dtypes')
            print (df2_dtypes)
            raise Exception('df1_dtypes != df2_dtypes')
    
    else:
        df1 = pd.read_csv(left_path)
        df2 = pd.read_csv(right_path)
        
        for c, df1_row in df1.iterrows():
            df2_row = df2.iloc[c]

            # Test that the two rows are identical using the pandas equality test            
            if not df1_row.equals(df2_row):
                print ('df1_row')
                print (df1_row)
                print ('df2_row')
                print (df2_row)
                raise Exception('df1_row != df2_row')
            
            
    return True