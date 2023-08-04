import os
import hazelbean as hb
import initialize_project
import utils

if __name__ == '__main__':
        
    ### ------- ENVIRONMENT SETTINGS -------------------------------

    # Users should only need to edit lines in this ENVIRONMENT SETTINGS section
    # Everything is relative to these (or the source code dir).
    # Specifically, 
    # 1. ensure that the project_dir makes sense for your machine
    # 2. ensure that the base_data_dir makes sense for your machine
    # 3. ensure that the data_credentials_path points to a valid credentials file, if relevant
    # 4. ensure that the input_bucket_name points to a cloud bucket you have access to, if relevant

    # A ProjectFlow object is created from the Hazelbean library to organize directories and enable parallel processing.
    # project-level variables are assigned as attributes to the p object (such as in p.base_data_dir = ... below)
    # The only agrument for a project flow object is where the project directory is relative to the current_working_directory.
    user_dir = os.path.expanduser('~')

    # Specify which extra dirs define where the project_dir will be
    extra_dirs = ['Files', 'Research', 'cge', 'gtappy', 'projects']
    
    # The project_name is used to name the project directory below. Also note that
    # ProjectFlow only calculates tasks that haven't been done yet, so adding 
    # a new project_name will give a fresh directory and ensure all parts
    # are run.
    project_name = 'test_gtappy_project'

    # The project-dir is where everything will be stored, in particular in an input, intermediate, or output dir
    # IMPORTANT NOTE: This should not be in a cloud-synced directory (e.g. dropbox, google drive, etc.), which
    # will either make the run fail or cause it to be very slow. The recommended place is (as coded above)
    # somewhere in the users's home directory.
    project_dir = os.path.join(user_dir, os.sep.join(extra_dirs), project_name)

    p = hb.ProjectFlow(project_dir)

    # The ProjectFlow object p will manage all tasks to be run, enables parallelization over spatial tiles or model runs,
    # manages directories, and provies a central place to store project-level variables (as attributes of p) that
    # works between tasks and between parallel threads. For instance, here we define the local variables above
    # to ProjectFlow attributes.
    p.user_dir = user_dir
    p.project_name = project_name
    p.project_dir = project_dir

    # Set the base data dir. The model will check here to see if it has everything it needs to run.
    # If anything is missing, it will download it. You can use the same base_data dir across multiple projects.
    # Additionally, if you're clever, you can move files generated in your tasks to the right base_data_dir
    # directory so that they are available for future projects and avoids redundant processing.
    # NOTE THAT the final directory has to be named base_data to match the naming convention on the google cloud bucket.
    # As with the project dir, this should be a non-cloud-synced directory, and ideally on a fast NVME SSD drive,
    # as this is primarily io-bound.
    p.base_data_dir = os.path.join('C:/Users/jajohns/Files/Research/base_data')


    # Define which aggregations will be used when GTAP is run.    
    fully_disaggregated_label = '65x141'
    shareable_and_fast_label = '10x10'    
    p.aggregation_labels = [shareable_and_fast_label]
    # p.aggregation_labels = [fully_disaggregated_label]
    # p.aggregation_labels = [shareable_and_fast_label, fully_disaggregated_label] # For use will non-test-set
    
    # Define which scenarios (shocks) will be run
    # p.experiment_labels = ['agpr20b']
    p.experiment_labels = ['agpr10', 'agpr20', 'agpr30']

    # Generate a nested dictionary for all permutations of aggregations and experiments. 
    # This will set xsets, xsubsets, and shocks attributes of the ProjectFlow object p.
    utils.set_attributes_based_on_aggregation_and_experiments(p, p.aggregation_labels, p.experiment_labels)


    ###------- Write the unique information that defines how each scenario's CMF is different.

    # Define AGGREGATION specific sets
    p.xsets['10x10'] = {
        'AGCOM': ['Agri commodities', '(GrainsCrops, MeatLstk, ProcFood)'],
        'AGCOM_SM': ['smaller agri commodities', '(GrainsCrops, MeatLstk)'],
    }
    p.xsubsets['10x10'] = [
        'AGCOM is subset of COMM', 
        'AGCOM is subset of ACTS', 
        'AGCOM_SM is subset of COMM', 
        'AGCOM_SM is subset of ACTS'
    ]
    
    # p.xsets['65x141'] = {
    #     'AGCOM': ['Agri commodities', '(pdr, wht, gro, v_f, osd, c_b, pfb, ocr, ctl, oap, rmk, wol)'],
    #     'AGCOM_SM' : ['smaller agri commodities', '(pdr, wht, gro)'],       
    # }
    # p.xsubsets['65x141'] = [
    #     'AGCOM is subset of COMM', 
    #     'AGCOM is subset of ACTS', 
    #     'AGCOM_SM is subset of COMM',
    #     'AGCOM_SM is subset of ACTS'
    # ]
    

    # Define scenario specific shocks   
    p.shocks['10x10']['agpr10'] = {
        'name': 'agri_productivity increases 10p',
        'shortname': 'agpr10',
        'shock_string': 'Shock aoall(AGCOM_SM, reg) = uniform 10;'
    }
 
    p.shocks['10x10']['agpr20'] = {
        'name': 'agri_productivity increases 20p',
        'shortname': 'agpr20',
        'shock_string': 'Shock aoall(AGCOM_SM, reg) = uniform 20;'
    }

    p.shocks['10x10']['agpr30'] = {
        'name': 'agri_productivity increases 30p',
        'shortname': 'agpr30',
        'shock_string': 'Shock aoall(AGCOM_SM, reg) = uniform 30;'
    }

    # p.shocks['65x141']['agpr20b']= {
    #     'name': 'agri_productivity increases 20p',
    #     'shortname': 'agpr20',
    #     'shock_string': 'Shock aoall(AGCOM_SM, reg) = uniform 20;'
    # }
 
    # p.shocks['65x141']['beef50']= {
    #     'name': 'agri_productivity increases 20p',
    #     'shortname': 'agpr20',
    #     'shock_string': 'Shock aoall(AGCOM_SM, reg) = uniform 20;'
    # }

    # p.shocks['65x141']['rtms5']= {
    #     'name': 'agri_productivity increases 20p',
    #     'shortname': 'agpr20',
    #     'shock_string': 'Shock aoall(AGCOM_SM, reg) = uniform 20;'
    # }    




     
    p.cge_model_release_string = 'gtap_v7_2022_08_04'
    p.cge_model_dir = os.path.join(p.base_data_dir, 'gtappy', 'cge_releases', p.cge_model_release_string)
    p.cge_data_dir = os.path.join(p.cge_model_dir, 'Model', 'gtapv7-cmd', 'data')    


    p.mapping_10_path = hb.get_first_extant_path(os.path.join('gtappy', 'aggregation_mappings', 'new_mapfile_data', 'mapping_10_regs_to_4_and_3_inc_categories.csv'), [p.input_dir, p.base_data_dir])
    p.mapping_141_path = None # Not needed, cause no remapping is done here.


    p.mapping_paths = {}
    p.mapping_paths[shareable_and_fast_label] = p.mapping_10_path
    p.mapping_paths[fully_disaggregated_label] = p.mapping_141_path
    
    p.gempack_utils_dir = "C://GP" # This is where the GEMPACK license is installed too.

    # Set the run_type, which will determine which task tree to load and execute.
    p.run_type = 'extract_and_run'
    # p.run_type = 'process_aez_results'
    
    
    initialize_project.run(p)
    
    

