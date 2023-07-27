import os
import hazelbean as hb
import food_systems_initialize_project
import gtap_functions
from gtap_invest import gtap_invest_integration_functions

if __name__ == '__main__':
        
    ### ------- ENVIRONMENT SETTINGS -------------------------------

    # Users should only need to edit lines in this ENVIRONMENT SETTINGS section
    # Everything is relative to these (or the source code dir).
    # Specifically, 
    # 1. ensure that the project_dir makes sense for your machine
    # 2. ensure that the base_data_dir makes sense for your machine
    # 3. ensure that the data_credentials_path points to a valid credentials file
    # 4. ensure that the input_bucket_name points to a cloud bucket you have access to

    # A ProjectFlow object is created from the Hazelbean library to organize directories and enable parallel processing.
    # project-level variables are assigned as attributes to the p object (such as in p.base_data_dir = ... below)
    # The only agrument for a project flow object is where the project directory is relative to the current_working_directory.
    user_dir = os.path.expanduser('~')
    script_dir = os.path.dirname(os.path.realpath(__file__))
    
    project_name = 'test_seals_project'
    project_dir = '../../projects/food_systems_alt'
    p = hb.ProjectFlow(project_dir)

    # The ProjectFlow object p will manage all tasks to be run, enables parallelization over spatial tiles or model runs,
    # manages directories, and provies a central place to store project-level variables (as attributes of p) that
    # works between tasks and between parallel threads. For instance, here we define the local variables above
    # to ProjectFlow attributes.
    p.user_dir = user_dir
    p.script_dir = script_dir
    p.project_name = project_name
    p.project_dir = project_dir

    # Set the base data dir. The model will check here to see if it has everything it needs to run.
    # If anything is missing, it will download it. You can use the same base_data dir across multiple projects.
    # Additionally, if you're clever, you can move files generated in your tasks to the right base_data_dir
    # directory so that they are available for future projects and avoids redundant processing.
    # NOTE THAT the final directory has to be named base_data to match the naming convention on the google cloud bucket.
    p.base_data_dir = os.path.join('G:/My Drive/Files/Research/base_data')

    # p = hb.ProjectFlow('../../projects/food_systems_alt')   
    
    
    fully_disaggregated_label = '65x141'
    shareable_and_fast_label = '10x10'
    
    p.aggregation_labels = [shareable_and_fast_label]
    # p.aggregation_labels = [fully_disaggregated_label]
    # p.aggregation_labels = [shareable_and_fast_label, fully_disaggregated_label] # For use will non-test-set
    
    p.experiment_labels = ['agpr20b', 'beef50', 'rtms5']
    
    p.cge_model_release_string = 'gtap_v7_2022_08_04'
    p.cge_model_dir = os.path.join(p.base_data_dir, 'gtappy', 'cge_releases', p.cge_model_release_string)
    p.cge_data_dir = os.path.join(p.cge_model_dir, 'Model', 'gtapv7-cmd', 'data')
    

    # START HERE: Make gtappy follow the dynamic base data loading pattern. Then check if I should
    # also replicate the way it creates things in the input dir on the first run. But
    # also start by cleaning repos (specifically gtap_invest ignore)
    check_path = os.path.join('gtappy', 'aggregation_mappings', 'new_mapfile_data', 'mapping_10_regs_to_4_and_3_inc_categories.csv')
    p.mapping_10_path = hb.get_first_extant_path(check_path, [p.input_dir, p.base_data_dir])
    # p.mapping_10_path = os.path.join(p.input_dir, 'new_mapfile_data', 'mapping_10_regs_to_4_and_3_inc_categories.csv')


    p.mapping_141_path = None
    p.mapping_paths = {}
    p.mapping_paths[shareable_and_fast_label] = p.mapping_10_path
    p.mapping_paths[fully_disaggregated_label] = p.mapping_141_path
    
    p.gempack_utils_dir = "C://GP"

    p.run_type = None
    
    if p.run_type is None:        
        p.extract_from_basedata_har_task = p.add_task(gtap_functions.extract_from_basedata_har)                
        p.create_mapfile_har_task = p.add_task(gtap_functions.create_mapfile_har)                
        p.run_gtap_v7_executable_task = p.add_task(gtap_functions.run_gtap_v7_executable)
        p.gtap_v7_results_summary_task = p.add_task(gtap_functions.gtap_v7_results_summary)

    food_systems_initialize_project.run(p)
    
    

