
import os
import hazelbean as hb
from gtappy import gtappy_file_io
from gtappy import gtappy_cmf_generation
from gtappy import gtappy_runner


import multiprocessing

import pandas as pd
import numpy as np


def base_data_as_csv(p):
    """Take a GTAP data release from the project input_dir or base_data and extract it into user-editable "indexed CSVs". 
    This will optionally also write the indexed CSVs back into a harfile to ensure there is no data loss and for
    later gempack usage."""

    if p.run_this:
        hb.timer()
        
        # GTAPPY runs for multiple aggregations and follows the philosophy that
        # for a well-built model, the results will be intuitively similar for different aggregations
        # and thus it serves as a decent check.
        # Here, for all aggergations listed, extract every har file present.
        for aggregation_label in p.aggregation_labels:    
            input_har_dir = os.path.join(p.cge_data_dir, aggregation_label)
            hars_to_look_for = hb.list_filtered_paths_nonrecursively(input_har_dir, include_extensions='.har')
            
            # Iterate through all the harfiles found in the aggregation directory
            for har_filename in hars_to_look_for:
                
                # Write har to Indexed-CSVs. See other functions, but note that Indexed CSV is a custom
                # filetype created for GTAPPY that is a single flat CSV describing the contents of a harfile that
                # have been extracted into a folder of the same name as the indexed CSV.
                input_har_path = har_filename    
                har_index_path = os.path.join(p.cur_dir, aggregation_label, hb.file_root(input_har_path) + '.csv')              
                if hb.path_exists(input_har_path):
                    hb.log('Found input_har_path: ' + input_har_path)
                    output_dir = os.path.join(p.cur_dir, aggregation_label)
                    hb.create_directories(output_dir)
                    
                    if not hb.path_exists(har_index_path, verbose=True): # Minor note, could add more robust file validation to check for ALL the implied files to exist.
                    
                        # Extract the har to the indexed DF format.
                        gtappy_file_io.har_to_indexed_dfs(input_har_path, har_index_path)                    
                        
                        # For validation (and actual use in the model), create a new har from the indexed dir.
                        local_har_path = hb.path_rename_change_dir(input_har_path, output_dir)
                        gtappy_file_io.indexed_dfs_to_har(har_index_path, local_har_path)



def mapfile(p):
    """In the TABLO code of a GTAP model, there often are additional HAR files 
    added that aggregate the base data for either shock definition or plotting of aggregated results. 
    Here, we create a mapfile that will summarize the above-defined aggregated data into "temporary"
    further aggregations that might be used for plotting results (e.g., income groups).
    Create a mapping file here based on the aggregation file present in the input dir."""
    
    if p.run_this:
        for aggregation_label in p.aggregation_labels:
                            
            # We will construct a new inded_dfs saved to mapfile.har that will be used for further aggregation
            # Start by basing it on the basedata.csv index file.
            input_indexed_dfs_path = os.path.join(p.base_data_as_csv_dir, aggregation_label, 'basedata.csv')
            input_indexed_dfs = pd.read_csv(input_indexed_dfs_path)

            # In the GEMPACK language, sets are used extensively as arguments to functions or for aggregation
            # We are going to add our new mapfile information to the existing sets list, which we
            # can extract from the indexed CSV.
            sets_list = gtappy_file_io.get_set_labels_from_index_path(input_indexed_dfs_path)

            # Generatea a "stub" csv which is all the information that should go into the mapping file 
            # that can be inferred from the base data (basically everything besides 
            # the actuall aggregation to a smaller set.)
            mapfile_stub_index_csv_path = os.path.join(p.cur_dir, aggregation_label, 'mapfile_stub.csv')
            if not hb.path_exists(mapfile_stub_index_csv_path):
                mapfile_data_dir = os.path.join(p.cur_dir, aggregation_label, 'MapFile')
                hb.create_directories(mapfile_data_dir)

                # Get rid of unnecessary metadata
                input_indexed_dfs = input_indexed_dfs.loc[input_indexed_dfs['header'].str.startswith('XX')]
                
                # Generate an index_dict, which will be convereted into a dataframe and then saved to Indexed CSV
                columns = input_indexed_dfs.columns
                index_dict = {i : list(input_indexed_dfs[i]) for i in columns} 
                
                # The new mapfile will be saved as an ndCSV (rename of Indexed CSV?)
                index_csv_path = os.path.join(p.cur_dir, aggregation_label, 'MapFile.csv')

                # Iterate through the sets, read them from the basedata ndCSV
                # and save them to into the new MapFile's ndCSV data dir
                for set_label in sets_list:
                    set_path = os.path.join(p.base_data_as_csv_dir, aggregation_label, 'basedata', set_label + '.csv')
                    set_df = pd.read_csv(set_path)

                    data_dict = {set_label: list(set_df[set_label])}
                    data_df = pd.DataFrame(data=data_dict)
                    data_path = os.path.join(mapfile_data_dir, set_label + '.csv')
                    data_df.to_csv(data_path, index=False)
                    
                # Write the ndCSV's Index file.
                for set_label in sets_list:
                    # trimmed_set_col = input_basedata_sets_dfs[set_label].dropna()
                    index_dict['header'].append(set_label)
                    index_dict['shape'].append(len(sets_list))
                    index_dict['long_name'].append('Set ' + str(set_label))
                    index_dict['dim_names'].append(set_label)
                    index_dict['ndims'].append(1)
                    index_dict['dtype'].append('<U12')
                    index_dict['coefficient_name'].append('')

                
                
                # Now, add new information to add in new mappings. This requires 
                # 1. adding an entry to the mapfile.csv index for the new set of labels
                # 2. adding an entry for the actual many to few mapping
                # 2. adding the data to the mapfile dir with a new csv                
                mapping_path = p.mapping_paths[aggregation_label]
                if mapping_path is not None:
                    mapping_df = pd.read_csv(mapping_path)
                    to_labels = []
                    
                    # Determine which columns in the mappings csv are the from and to labels
                    # This is implied by if it is in the base_data index or not. 
                    # Note that this means that all mappings must an aggregation of the base data.
                    for col_name in mapping_df.columns:
                        if col_name in index_dict['header']:
                            from_label = col_name
                        else:
                            to_labels.append(col_name)
                
                    # Add the new mapping SETS to the index
                    for col_name in to_labels:
                        n_uniques = len(mapping_df[col_name].unique())
                        index_dict['header'].append(col_name)
                        index_dict['shape'].append(str(n_uniques))
                        index_dict['long_name'].append('Set ' + col_name + ' aggregation')
                        index_dict['dim_names'].append(col_name)
                        index_dict['ndims'].append(1)
                        index_dict['dtype'].append('<U5')
                        index_dict['coefficient_name'].append('')
                        
                        data_df = pd.DataFrame(mapping_df[col_name].unique())
                        data_dir = os.path.join(p.cur_dir, aggregation_label, 'MapFile')
                        file_path = os.path.join(data_dir, col_name + '.csv')
                        data_df.to_csv(file_path, index=False)
                        
                        # uniques_with_nans = list(mapping_df[col_name].unique() )
                        # n_new_nans_required = len(mapping_df[col_name]) - n_uniques
                        # nans = list(np.asarray([np.nan] * n_new_nans_required))
                        # sets_dict[col_name] = uniques_with_nans + nans
                    
                    # Add the new mapping correspondences to the index
                    hack_mapping_names = ['MINC', 'MRIN']
                    for c, col_name in enumerate(to_labels):
                        n_uniques = len(mapping_df[col_name].unique())
                        index_dict['header'].append(hack_mapping_names[c])
                        index_dict['shape'].append(len(mapping_df[from_label]))
                        index_dict['long_name'].append('Mapping ' + col_name + ' from ' + from_label + '(' + str(len(mapping_df[from_label])) + ') to ' + col_name + '(' + str(n_uniques) + ')')
                        index_dict['dim_names'].append(from_label)
                        index_dict['ndims'].append(1)
                        index_dict['dtype'].append('<U12')
                        index_dict['coefficient_name'].append('')
                    
                        data_df = pd.DataFrame(mapping_df[col_name])
                        data_dir = os.path.join(p.cur_dir, aggregation_label, 'MapFile')
                        file_path = os.path.join(data_dir, hack_mapping_names[c] + '.csv')
                        data_df.to_csv(file_path, index=False)
                        
                        # sets_dict[hack_mapping_names[c]] = mapping_df[col_name]
                    
                    # Write the header data to CSVs in the indexed folder
                    
                    
                    index_df = pd.DataFrame(data=index_dict)
                    index_df.to_csv(index_csv_path, index=False)
                    # hb.python_object_to_csv(sets_dict, sets_csv_path, csv_type='2d_odict_list')
                            
                    mapfile_har_path = os.path.join(p.cur_dir, aggregation_label, 'MapFile.har')
                    if not hb.path_exists(mapfile_har_path):

                        ## START HERE: Almost there. Make it sothat the new Harfiole also has the remappings that Erwin calls in the code, eg.g rin4, rindc, minc, mrin.                
                        gtappy_file_io.indexed_dfs_to_har(index_csv_path, mapfile_har_path)
                          
 
def gtap_runs(p):
    """Run a precompiled gtap exe file by creating a cmf file and calling it.
    
    The current general approach to ingesting a new gtap-aez release from Purdue is:
    
    1. Extract it into the cge_releases dir and update the release name in the run file.
    2. Generate a cmf_dict with a scenario-derived bat_to_catear_vars_replace_dict that converts p1, p2, p3, p4, p5 to the appropriate paths.
    """


    if p.run_this:
        
        for aggregation_label in p.aggregation_labels:

            run_parallel = 0
            parallel_iterable = []
             
            for experiment_label in p.experiment_labels:
                
                for n_years_counter, ending_year in enumerate(p.years):
                    
                    if n_years_counter == 0:
                        starting_year = p.base_year
                    else:
                        starting_year = p.years[n_years_counter - 1]
                        
                    output_dir = os.path.join(p.cur_dir, aggregation_label, experiment_label, str(ending_year))
                    expected_sl4_path = os.path.join(output_dir, experiment_label + '_' + str(ending_year) + '.sl4')
                    
                    if not hb.path_exists(expected_sl4_path):
                        
                        hb.create_directories(output_dir)
                        
                        generated_cmf_path = os.path.join(output_dir, aggregation_label + '_' + experiment_label + '.cmf')

                        if n_years_counter == 0:
                            current_cge_data_dir = os.path.join(p.cge_data_dir, aggregation_label)
                        else:
                            current_cge_data_dir = os.path.join(p.cur_dir, aggregation_label, experiment_label, str(starting_year))
                        
                        # CMF: experiment_label # Rename BUT I understand this one might not be changeable because it appears to be defined by the filename of the CMF?
                        # p1: starting_data_dir # EXCLUDE THIS, because we only use it for p2
                        # p2: starting_data_file_path # Rename points to the correct starting har
                        # p3: output_dir # Rename
                        # p4: starting_year # Rename
                        # p5: ending_year # Rename
                        # TODOO Once i get erwin's renamed bat file this step can go away
                        bat_to_catear_vars_replace_dict = {}
                        bat_to_catear_vars_replace_dict['<'] = '<^'
                        bat_to_catear_vars_replace_dict['>'] = '^>' 
                        bat_to_catear_vars_replace_dict['<^CMF^>'] = '<^cmf^>' 
                        bat_to_catear_vars_replace_dict['<^cmf^>'] = '<^experiment_label^>'                    
                        bat_to_catear_vars_replace_dict['<^p1^>'] = '<^starting_data_dir^>' 
                        bat_to_catear_vars_replace_dict['<^p2^>'] = '<^starting_data_file_path^>' 
                        bat_to_catear_vars_replace_dict['<^p3^>'] = '<^output_dir^>' 
                        bat_to_catear_vars_replace_dict['<^p4^>'] = '<^starting_year^>' 
                        bat_to_catear_vars_replace_dict['<^p5^>'] = '<^ending_year^>' 
                        
                        labeled_cmf_template_path = os.path.join(output_dir, 'labeled_cmf_template.cmf')
                        hb.replace_in_file_via_dict(p.template_cmf_path, labeled_cmf_template_path, bat_to_catear_vars_replace_dict)
                        
                        
                        initial_year = 1 # TODOO reference run file
                        if initial_year:
                            bat_to_catear_vars_replace_dict['<^p1^>'] = os.path.join(p.cge_data_dir, aggregation_label)
                            bat_to_catear_vars_replace_dict['<^p2^>'] = os.path.join(bat_to_catear_vars_replace_dict['<^p1^>'], 'basedata.har') 
                        else:
                            bat_to_catear_vars_replace_dict['<^p1^>'] = os.path.join(p.cur_dir, aggregation_label, experiment_label)
                            bat_to_catear_vars_replace_dict['<^p2^>'] = os.path.join(bat_to_catear_vars_replace_dict['<^p1^>'], experiment_label + '_' + starting_year + '.upd') 
                        bat_to_catear_vars_replace_dict['<^p3^>'] = bat_to_catear_vars_replace_dict['<^p1^>']
                        bat_to_catear_vars_replace_dict['<^p4^>'] = starting_year
                        bat_to_catear_vars_replace_dict['<^p5^>'] = ending_year

                        # bat_to_catear_vars_replace_dict['<^CMF^>'] = '<^cmf^>' # MOVE THIS TO AUTO

                        
                        
                        
                        

                        cmf_dict = gtappy_cmf_generation.generate_cmf_dict_from_cmf_file(p.template_cmf_path, replace_dict=bat_to_catear_vars_replace_dict)
                    
                        hb.log('Generating cmf for ', aggregation_label, experiment_label, ' with bat_to_catear_vars_replace_dict: ', bat_to_catear_vars_replace_dict, '\n', hb.print_iterable(cmf_dict))
                        
                        cmf_dict['xSets'] = p.xsets[aggregation_label]
                        cmf_dict['xSubsets'] = p.xsubsets[aggregation_label]
                        cmf_dict['Shocks'] = p.shocks[aggregation_label][experiment_label]
                        cmf_dict['cmf_commands'] = p.cmf_commands[aggregation_label][experiment_label]

                        
                        for replace_k, replace_v in cmf_dict['cmf_commands'].items():
                            cmf_dict[replace_k] = replace_v

                        hb.log('ADDED to the cmf for ', aggregation_label, experiment_label, ' to get',  hb.print_iterable(cmf_dict))

                        
                        

                        gtappy_cmf_generation.generate_cmf_file_for_scenario(cmf_dict, 
                                                                    experiment_label,                                                    
                                                                    current_cge_data_dir,
                                                                    output_dir,     
                                                                    generated_cmf_path,     )
                        
                        
                        # cge_executable_path = os.path.join(p.cge_model_dir, 'mod\\GTAPV7-AEZ.exe')
                        
                        # Generate the OS call for the CGE model executable and its corresponding cmf file
                        call_list = [p.cge_executable_path, '-cmf', generated_cmf_path]                
                        
                        if run_parallel: # When running in paralell, add it to a list for later parallel processing.
                            parallel_iterable.append(tuple([experiment_label, call_list]))

                        else: # Because not running in parallel, just run it right away.                    
                            call_list = [p.cge_executable_path, '-cmf', generated_cmf_path]
                            gtappy_runner.run_gtap_cmf(generated_cmf_path, call_list)

                # Now that the iterable is created, run them all in parallel
                num_workers = len(p.experiment_labels)
                if run_parallel:
                    # Performance note: it takes about 3 seconds to run this block even with nothing in the iterable, I guess just from launching the worker pool
                    if len(parallel_iterable) > 0:

                        worker_pool = multiprocessing.Pool(num_workers)  # NOTE, worker pool and results are LOCAL variabes so that they aren't pickled when we pass the project object.

                        finished_results = []
                        result = worker_pool.starmap_async(gtappy_runner.run_gtap_cmf, parallel_iterable)
                        for i in result.get():
                            finished_results.append(i)
                        worker_pool.close()
                        worker_pool.join()


                
        # Note bug. Currently even when done in parallel, only 1 run can be run per python run. Says it runs out of space to allocate results, which isn't true. I'm assuming is an unreleased shared asset, maybe gtap.exe

def results_as_csv(p):
    
    if p.run_this:
        
        for aggregation_label in p.aggregation_labels:
            
            for experiment_label in p.experiment_labels:
                
                expected_filenames = [experiment_label + '.sl4',  experiment_label + '.UPD', experiment_label + '-SUM.har', experiment_label + '-VOL.har', experiment_label + '-WEL.har',]
                
                for filename in expected_filenames:
                    hb.log('Extracting data for ', aggregation_label, experiment_label, filename)
                    
                    experiment_dir = os.path.join(p.gtap_runs_dir, aggregation_label, experiment_label)
                    expected_path = os.path.join(experiment_dir, filename)
                    
                    if not hb.path_exists(expected_path, verbose=True):
                        raise NameError('Cannot find file: ' + str(expected_path))
                    
                    indexed_df_path = os.path.join(p.cur_dir, aggregation_label, experiment_label, filename.replace('.', '_') + '.csv')
                    if not hb.path_exists(indexed_df_path):     
                        
                        # START HERE: See if using the sl4 interface makes the sl4 pull in all the actually-used data.
                        if os.path.splitext(filename)[1] == '.sl4':
                            gtappy_file_io.sl4_to_indexed_dfs(expected_path, indexed_df_path)
                        else:
                            gtappy_file_io.har_to_indexed_dfs(expected_path, indexed_df_path)
                        
                        
                    
                    # har_path = os.path.join(p.cur_dir, aggregation_label, experiment_label, experiment_label + '.har')
                    har_path = hb.path_replace_extension(indexed_df_path, '.har')
                    if not hb.path_exists(har_path) and os.path.splitext(filename)[1] != '.sl4'  and os.path.splitext(filename)[1] != '.UPD':
                        gtappy_file_io.indexed_dfs_to_har(indexed_df_path, har_path) 

    

def vizualization(p):
    if p.run_this:
        plots = {}

        vars_to_plot = p.reg_vars_to_plot

        # DO INDIVIDUAL EXPERMINET PLOTS        
        for aggregation_label in p.aggregation_labels:            
            for experiment_label in p.experiment_labels:
                current_sl4_dir = os.path.join(p.results_as_csv_dir, aggregation_label, experiment_label, experiment_label + '_sl4')
                for c, var in enumerate(vars_to_plot):
                    output_csv_path = os.path.join(p.cur_dir, aggregation_label, experiment_label, var + '.csv')
                    
                    if hb.path_exists(output_csv_path):
                        current_var_path = os.path.join(current_sl4_dir, var + '.csv')
                        df = pd.read_csv(current_var_path)

                        
                        hb.create_directories(output_csv_path)
                        output_png_path = os.path.join(p.cur_dir, aggregation_label, experiment_label, var + '.png')

                        hb.df_plot(df, output_png_path, type='bar')
                        
                        # Write csv
                        df.to_csv(output_csv_path, index=False)


        # DO CROSS-EXPERIMENT PLOTS
        for aggregation_label in p.aggregation_labels:    
            for c, var in enumerate(vars_to_plot):
                for cc, experiment_label in enumerate(p.experiment_labels):
                    current_sl4_dir = os.path.join(p.results_as_csv_dir, aggregation_label, experiment_label, experiment_label + '_sl4')
                    current_var_path = os.path.join(current_sl4_dir, var + '.csv')
                    
                    if cc == 0:
                        df = pd.read_csv(current_var_path)
                    else:
                        new_df = pd.read_csv(current_var_path)
                        df = hb.df_merge(df, new_df)