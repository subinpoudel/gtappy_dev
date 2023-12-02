import os, sys
sys.path.insert(0, '../') # For files run WITHIN the package dir, need to fix the relative structure

import hazelbean as hb
import gtappy 
from gtappy import gtappy_file_io
import pandas as pd




# Get user dir
user_dir = os.path.expanduser('~')
base_data_dir = os.path.join(user_dir, 'Files', 'base_data')

script_dir = os.path.dirname(os.path.realpath(__file__))
test_output_dir = os.path.join(script_dir, 'test_output')
hb.create_directories(test_output_dir)

remove_at_exit = False # This wont work because we make many files.
cleanup_test_dir = 0

gtappy_base_data_har_path = os.path.join(base_data_dir, "gtappy/cge_releases/gtapv7-aez_20231103/data/v11-s26-r50_20231103/basedata.har")

# CURRENT LIMITATION!!! Failes on converting this UPD file becuase it cannot find LCOV. 
upd_path = os.path.join(base_data_dir, "gtappy/cge_releases/gtapv7-aez_20231103/out/TarElimProd.UPD")

# CURRENT LIMITATION!!! Can convert a sl4 to an indexed df, but cannot convert an indexed df back to a sl4. This is because
# I think, sl4's get around the 4-character header constraint by assigning a 4 digit int to the header name and then saving the 
# header name in the
sl4_path = os.path.join(base_data_dir, "gtappy/cge_releases/gtapv7-aez_20231103/out/TarElim.sl4")
# This is fixed by just treating it as a har
paths_to_test = [gtappy_base_data_har_path, sl4_path]

for path in paths_to_test:
    path_extension = os.path.splitext(path)[1]
    if path.endswith('.har') or path.endswith('.UPD'):
        har_as_csv_path = os.path.join(test_output_dir, hb.file_root(path) + '_as_csv.csv')
        gtappy_file_io.har_to_indexed_dfs(path, har_as_csv_path)

        csv_back_as_har_path = os.path.join(test_output_dir, hb.file_root(path) + '_back_as_har' + path_extension)
        gtappy_file_io.indexed_dfs_to_har(har_as_csv_path, csv_back_as_har_path)

        har_back_as_csv_path = os.path.join(test_output_dir, hb.file_root(path) + '_back_as_csv.csv')
        gtappy_file_io.har_to_indexed_dfs(csv_back_as_har_path, har_back_as_csv_path)

        # Assert that a df created directly from the har is identical to the df created from a har created from the initial df.
        gtappy_file_io.assert_two_indexed_csv_paths_are_identical(har_as_csv_path, har_back_as_csv_path)

    elif path.endswith('.sl4'):
        har_as_csv_path = os.path.join(test_output_dir, hb.file_root(path) + '_as_sl4_csv.csv')
        # gtappy_file_io.sl4_to_indexed_dfs(path, har_as_csv_path)
        gtappy_file_io.sl4_to_indexed_dfs(path, har_as_csv_path)

        # csv_back_as_har_path = os.path.join(test_output_dir, hb.file_root(path) + '_back_as_har' + path_extension)
        # gtappy_file_io.indexed_dfs_to_har(har_as_csv_path, csv_back_as_har_path)

        # har_back_as_csv_path = os.path.join(test_output_dir, hb.file_root(path) + '_back_as_csv.csv')
        # gtappy_file_io.har_to_indexed_dfs(csv_back_as_har_path, har_back_as_csv_path)

        # Assert that a df created directly from the har is identical to the df created from a har created from the initial df.
        # gtappy_file_io.assert_two_indexed_csv_paths_are_identical(har_as_csv_path, har_back_as_csv_path)
        
    else:
        print('Unhandled file type: ' + path)

csv_sets = gtappy_file_io.get_set_labels_from_index_path(har_as_csv_path)

if cleanup_test_dir:
    hb.remove_dirs(test_output_dir, safety_check='delete')
    
print('Test successful.')
5