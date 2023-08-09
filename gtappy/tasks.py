
import os
import hazelbean as hb
import file_io
import cmf_generation
import runner


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
                    p.L.info('Found input_har_path: ' + input_har_path)
                    output_dir = os.path.join(p.cur_dir, aggregation_label)
                    hb.create_directories(output_dir)
                    
                    if not hb.path_exists(har_index_path, verbose=True): # Minor note, could add more robust file validation to check for ALL the implied files to exist.
                    
                        # Extract the har to the indexed DF format.
                        file_io.har_to_indexed_dfs(input_har_path, har_index_path)                    
                        
                        # For validation (and actual use in the model), create a new har from the indexed dir.
                        local_har_path = hb.path_rename_change_dir(input_har_path, output_dir)
                        file_io.indexed_dfs_to_har(har_index_path, local_har_path)



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
            sets_list = file_io.get_set_labels_from_index_path(input_indexed_dfs_path)

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
                        file_io.indexed_dfs_to_har(index_csv_path, mapfile_har_path)
                          
 
def gtap_runs(p):
    """Run a precompiled gtap exe file by creating a cmf file and calling it.
    """


    if p.run_this:
        
        for aggregation_label in p.aggregation_labels:

            run_parallel = 1
            parallel_iterable = []
             
            for experiment_label in p.experiment_labels:
                expected_sl4_path = os.path.join(p.cur_dir, aggregation_label, experiment_label, experiment_label + '.sl4')
                
                if not hb.path_exists(expected_sl4_path):
                
                    inputs_dict = cmf_generation.gtap_v7_cmf_dict.copy()
                    
                    inputs_dict['xSets'] = p.xsets[aggregation_label]
                    inputs_dict['xSubsets'] = p.xsubsets[aggregation_label]
                    inputs_dict['Shocks'] = p.shocks[aggregation_label][experiment_label]

                    
                    data_dir = os.path.join(p.cge_model_dir, 'Model', 'gtapv7-cmd', 'data', aggregation_label)
                    hb.create_directories(data_dir)

                    output_dir = os.path.join(p.cur_dir, aggregation_label, experiment_label)
                    hb.create_directories(output_dir)
                    
                    generated_cmf_path = os.path.join(p.cur_dir, aggregation_label + '_' + experiment_label + '.cmf')



                    cmf_generation.generate_cmf_file_for_scenario(inputs_dict, 
                                                                experiment_label,                                                    
                                                                data_dir,
                                                                output_dir,     
                                                                generated_cmf_path,     )
                    
                    
                    cge_executable_path = os.path.join(p.cge_model_dir, 'Model\\gtapv7-cmd\\mod\\GTAPV7.exe')
                    
                    # Generate the OS call for the CGE model executable and its corresponding cmf file
                    call_list = [cge_executable_path, '-cmf', generated_cmf_path]                
                    
                    if run_parallel: # When running in paralell, add it to a list for later parallel processing.
                        parallel_iterable.append(tuple([experiment_label, call_list]))

                    else: # Because not running in parallel, just run it right away.                    
                        call_list = [cge_executable_path, '-cmf', generated_cmf_path]
                        runner.run_gtap_cmf(generated_cmf_path, call_list)

            # Now that the iterable is created, run them all in parallel
            num_workers = len(p.experiment_labels)
            if run_parallel:
                # Performance note: it takes about 3 seconds to run this block even with nothing in the iterable, I guess just from launching the worker pool
                if len(parallel_iterable) > 0:

                    worker_pool = multiprocessing.Pool(num_workers)  # NOTE, worker pool and results are LOCAL variabes so that they aren't pickled when we pass the project object.

                    finished_results = []
                    result = worker_pool.starmap_async(runner.run_gtap_cmf, parallel_iterable)
                    for i in result.get():
                        finished_results.append(i)
                    worker_pool.close()
                    worker_pool.join()


                
        # Note bug. Currently even when done in parallel, only 1 run can be run per python run. Says it runs out of space to allocate results, which isn't true. I'm assuming is an unreleased shared asset, maybe gtap.exe
                    
                    
                    
                    
                    
                    
        # os.chdir(old_cwd)
        
        # scenario_labels = ['agpr20b']
        
        # for scenario_label in scenario_labels:
        #     cmf_path = os.path.join(p.cge_model_dir, 'Model\\gtapv7-cmd\\cmf\\' + scenario_label + '.CMF')
        #     sl4_path = os.path.join(p.cge_model_dir, 'Model\\gtapv7-cmd\\out\\' + scenario_label + '.sl4')
            
        #     if not hb.path_exists(sl4_path, verbose=True):
        #         call_list = [cge_executable_path, '-cmf', cmf_path]
        #         gtap_invest_integration_functions.run_gtap_cmf(cmf_path, call_list)
                
        # r"C:\Users\jajohns\Files\Research\cge\food_systems\projects\manuscript_v1\input\food_systems_2022_08_04\Model\gtapv7-cmd\mod\GTAPV7.exe"
        # r"C:\Users\jajohns\Files\Research\cge\food_systems\projects\manuscript_v1\input\food_systems_2022_08_04\Model\gtapv7-cmd\cmf\agpr20.CMF"
        # # r"C:\Users\jajohns\Files\Research\cge\food_systems\projects\manuscript_v1\input\food_systems_2022_08_04\Model\gtapv7-cmd\out\agpr20.sl4"
        
        # gtap_policy_baseline_solution_file_path = os.path.join(p.cur_dir, p.cge_model_dir, 'work', gtap_policy_baseline_scenario_label + '.sl4')
        # if not hb.path_exists(gtap_policy_baseline_solution_file_path, verbose=True):
        #     # Generate a new cmf file with updated paths.
        #     gtap_invest_integration_functions.generate_policy_baseline_cmf_file(gtap_policy_baseline_scenario_source_cmf_path, gtap_policy_baseline_scenario_cmf_path)

        #     # Run the gtap executable pointing to the new cmf file
        #     call_list = [gtapaez_executable_abs_path, '-cmf', gtap_policy_baseline_scenario_cmf_path]
        #     gtap_invest_integration_functions.run_gtap_cmf(gtap_policy_baseline_scenario_label, call_list)


        # # Define paths for the source cmf file (extracted from GTAP-AEZ integration zipfile) and the modified one that will be run
        # gtap_policy_baseline_scenario_label = str(p.base_year) + '_' + str(p.policy_base_year)[2:] + '_BAU'
        # gtap_policy_baseline_scenario_source_cmf_path = os.path.join(p.gtap1_aez_invest_local_model_dir, 'cmfs', gtap_policy_baseline_scenario_label + '.cmf')
        # gtap_policy_baseline_scenario_cmf_path = os.path.join(p.gtap1_aez_invest_local_model_dir, gtap_policy_baseline_scenario_label + '_local.cmf')
        # gtap_policy_baseline_solution_file_path = os.path.join(p.cur_dir, p.gtap_aez_invest_release_string, 'work', gtap_policy_baseline_scenario_label + '.sl4')

        # L.info('gtap_policy_baseline_scenario_cmf_path', gtap_policy_baseline_scenario_cmf_path)
        # L.info('gtap_policy_baseline_solution_file_path', gtap_policy_baseline_solution_file_path)

        # if not hb.path_exists(gtap_policy_baseline_solution_file_path, verbose=True):
        #     # Generate a new cmf file with updated paths.
        #     gtap_invest_integration_functions.generate_policy_baseline_cmf_file(gtap_policy_baseline_scenario_source_cmf_path, gtap_policy_baseline_scenario_cmf_path)

        #     # Run the gtap executable pointing to the new cmf file
        #     call_list = [gtapaez_executable_abs_path, '-cmf', gtap_policy_baseline_scenario_cmf_path]
        #     gtap_invest_integration_functions.run_gtap_cmf(gtap_policy_baseline_scenario_label, call_list)

        # run_parallel = 1
        # parallel_iterable = []
        # for gtap_scenario_label in p.gtap1_scenario_labels:

        #     current_scenario_source_cmf_path = os.path.join(p.gtap1_aez_invest_local_model_dir, 'cmfs', gtap_scenario_label + '.cmf')
        #     current_scenario_cmf_path = os.path.join(p.gtap1_aez_invest_local_model_dir, gtap_scenario_label + '_local.cmf')

        #     current_solution_file_path = os.path.join(p.cur_dir, p.gtap_aez_invest_release_string, 'work', gtap_scenario_label + '.sl4')

        #     # Hack to fix uris scenario typo
        #     src = os.path.join(p.gtap1_aez_invest_local_model_dir, 'cmfs', '2021_30_SR_RnD_20p_PESGB_30_allES.cmf')
        #     dst = os.path.join(p.gtap1_aez_invest_local_model_dir, 'cmfs', '2021_30_SR_RnD_20p_PESGC_30_allES.cmf')
        #     hb.copy_shutil_flex(src, dst)

        #     possible_file_names = [current_solution_file_path]
        #     if 'PESGC' in current_solution_file_path:
        #         possible_file_names.append(current_solution_file_path.replace('PESGC', 'PESGB'))


        #     if not any([hb.path_exists(i, verbose=True) for i in possible_file_names]):
        #         # Generate a new cmf file with updated paths.
        #         # Currently this just uses the policy_baseline version.
        #         gtap_invest_integration_functions.generate_policy_baseline_cmf_file(current_scenario_source_cmf_path, current_scenario_cmf_path)

        #         # Run the gtap executable pointing to the new cmf file
        #         call_list = [gtapaez_executable_abs_path, '-cmf', current_scenario_cmf_path]
        #         parallel_iterable.append(tuple([gtap_scenario_label, call_list]))

        #         # If the model is run sequentially, just call it here.
        #         if not run_parallel:
        #             gtap_invest_integration_functions.run_gtap_cmf(gtap_scenario_label, call_list)


        # if run_parallel:
        #     # Performance note: it takes about 3 seconds to run this block even with nothing in the iterable, I guess just from launching the worker pool
        #     if len(parallel_iterable) > 0:

        #         worker_pool = multiprocessing.Pool(p.num_workers)  # NOTE, worker pool and results are LOCAL variabes so that they aren't pickled when we pass the project object.

        #         finished_results = []
        #         result = worker_pool.starmap_async(gtap_invest_integration_functions.run_gtap_cmf, parallel_iterable)
        #         for i in result.get():
        #             finished_results.append(i)
        #         worker_pool.close()
        #         worker_pool.join()









        # ### R POSTSIM WORK



        # # Now call the R code to pull these
        # src_r_postsim_script_path = os.path.join(p.gtap1_aez_invest_local_model_dir, 'postsims', '01_output_csv.r')

        # # Create a local copy of the R file with modifications specific to this run (i.e., the shanging the workspace)
        # r_postsim_script_path = os.path.join(p.gtap1_aez_invest_local_model_dir, 'postsims', '01_output_csv_local.r')



        # # Copy the time-stamped gtap2 results to a non-time-stamped version to clarify which one to use for eg plotting.
        # p.gtap1_results_path = os.path.join(p.cur_dir, 'GTAP_Results.csv')
        # p.gtap1_land_use_change_path = os.path.join(p.cur_dir, 'GTAP_AEZ_LCOVER_ha.csv')
        # current_date = hb.pretty_time('year_month_day_hyphens')
        # dated_expected_files = [os.path.join(p.cur_dir, current_date +'_GTAP_Results.csv'), os.path.join(p.cur_dir, current_date +'_GTAP_AEZ_LCOVER_ha.csv')]
        # expected_files = [p.gtap1_results_path, p.gtap1_land_use_change_path]

        # for c, file_path in enumerate(dated_expected_files):
        #     if hb.path_exists(file_path):
        #         hb.copy_shutil_flex(file_path, expected_files[c])

        # gtap1_aez_results_exist = all([True if hb.path_exists(i) else False for i in expected_files])

        # if not gtap1_aez_results_exist:
        #     p.L.info('Starting r script at ' + str(r_postsim_script_path) + ' to create output files.')

        #     os.makedirs(os.path.join(os.path.split(src_r_postsim_script_path)[0], 'temp'), exist_ok=True)
        #     os.makedirs(os.path.join(os.path.split(src_r_postsim_script_path)[0], 'temp', 'merge'), exist_ok=True)

        #     # TODOO, This is a silly duplication of non-small files that could be eliminated. originally it was in so that i didn't have to modify Uris' r code.
        #     # TODOO Also, most of this R code is just running har2csv.exe, which is a license-constrained gempack file. replace with python har2csv
        #     hb.copy_file_tree_to_new_root(os.path.join(p.gtap1_aez_invest_local_model_dir, 'work'), os.path.join(p.gtap1_aez_invest_local_model_dir, 'postsims', 'in', 'gtap'))

        #     # working_dir = implied_r_working_dir = os.path.split(src_r_script_path)[0]
        #     gtap_invest_integration_functions.generate_postsims_r_script_file(src_r_postsim_script_path, r_postsim_script_path)

        #     hb.execute_r_script(p.r_executable_path, os.path.abspath(r_postsim_script_path))


        #     # The two CSVs generated by the script file are key outputs. Copy them to the cur_dir root as well as the output dir
        #     files_to_copy = [os.path.join(p.gtap1_aez_invest_local_model_dir, 'postsims', 'out', os.path.split(i)[1]) for i in dated_expected_files]

        #     for file_path in files_to_copy:
        #         hb.copy_shutil_flex(file_path, os.path.join(p.cur_dir, os.path.split(file_path)[1]), verbose=True)
        #         hb.copy_shutil_flex(file_path, os.path.join(p.output_dir, 'gtap1_aez', os.path.split(file_path)[1]), verbose=True)
        #         hb.copy_shutil_flex(file_path, os.path.join(p.cur_dir, os.path.split(expected_files[c])[1]), verbose=True)



def results_summary(p):
    
    if p.run_this:
        
        for aggregation_label in p.aggregation_labels:
            
            for experiment_label in p.experiment_labels:
                
                expected_filenames = [experiment_label + '.sl4',  experiment_label + '.UPD', experiment_label + '-SUM.har', experiment_label + '-VOL.har', experiment_label + '-WEL.har',]
                
                for filename in expected_filenames:
                    p.L.info('Extracting data for ', aggregation_label, experiment_label, filename)
                    
                    experiment_dir = os.path.join(p.gtap_runs_dir, aggregation_label, experiment_label)
                    expected_path = os.path.join(experiment_dir, filename)
                    
                    if not hb.path_exists(expected_path, verbose=True):
                        raise NameError('Cannot find file: ' + str(expected_path))
                    
                    indexed_df_path = os.path.join(p.cur_dir, aggregation_label, experiment_label, filename.replace('.', '_') + '.csv')
                    if not hb.path_exists(indexed_df_path):     
                        
                        # START HERE: See if using the sl4 interface makes the sl4 pull in all the actually-used data.
                        if os.path.splitext(filename)[1] == '.sl4':
                            file_io.sl4_to_indexed_dfs(expected_path, indexed_df_path)
                        else:
                            file_io.har_to_indexed_dfs(expected_path, indexed_df_path)
                        
                        
                    
                    # har_path = os.path.join(p.cur_dir, aggregation_label, experiment_label, experiment_label + '.har')
                    har_path = hb.path_replace_extension(indexed_df_path, '.har')
                    if not hb.path_exists(har_path) and os.path.splitext(filename)[1] != '.sl4K'  and os.path.splitext(filename)[1] != '.UPKD':
                        file_io.indexed_dfs_to_har(indexed_df_path, har_path) 

    

def process_results(p):
    
    if p.run_this:
        
        for aggregation_label in p.aggregation_labels:
            
            for experiment_label in p.experiment_labels:

                5