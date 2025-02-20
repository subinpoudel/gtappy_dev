import os
import hazelbean as hb
import gtappy.gtappy_initialize_project as gtappy_initialize_project
import gtappy.gtappy_utils as gtappy_utils

if __name__ == '__main__':
    
    ### ----- CHANGE LOG ---------------------------------
    """

    """


    ### ------- ENVIRONMENT SETTINGS -------------------------------

    # Users should only need to edit lines in this ENVIRONMENT SETTINGS section
    # Everything is relative to these (or the source code dir).
    # Specifically, 
    # 1. ensure that the project_dir makes sense for your machine
    # 2. ensure that the base_data_dir makes sense for your machine
    # 3. ensure that the data_credentials_path points to a valid credentials file, if relevant
    # 4. ensure that the input_bucket_name points to a cloud bucket you have access to, if relevant

    # A ProjectFlow object is created from the Hazelbean library to organize directories and enable parallel processing.
    # project-level variables are assigned as attributes to the p object (such as iawful awful time stripping stringsawful awful time stripping stringsn p.base_data_dir = ... below)
    # The only agrument for a project flow object is where the project directory is relative to the current_working_directory.
    user_dir = os.path.expanduser('~')

    # Specify which extra dirs define where the project_dir will be
    extra_dirs = ['Files', 'gtappy', 'projects']

    # The project_name is used to name the project directory below. Also note that
    # ProjectFlow only calculates tasks that haven't been done yet, so adding 
    # a new project_name will give a fresh directory and ensure all parts
    # are run.
    project_name = 'gtappy_gtap_aez_rd_3'

    # The project-dir is where everything will be stored, in particular in an input, intermediate, or output dir
    # IMPORTANT NOTE: This should not be in a cloud-synced directory (e.g. dropbox, google drive, etc.), which
    # will either make the run fail or cause it to be very slow. The recommended place is (as coded above)
    # somewhere in the users's home directory.
    project_dir = os.path.join(user_dir, os.sep.join(extra_dirs), project_name)
    
    p = hb.ProjectFlow(project_dir)
    p.test_mode = 1
    gtappy_initialize_project.build_extract_and_run_aez_task_tree(p)

    # The ProjectFlow object p will manage all tasks to be run, enables parallelization over spatial tiles or model runs,
    # manages directories, and provies a central place to store project-level variables (as attributes of p) that
    # works between tasks and between parallel threads. For instance, here we define the local variables above
    # to ProjectFlow attributes.
    # TODOO Could put this in the projectflow __init__ function.
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
    
    
    
    # Set the base data dir.  
    p.base_data_dir = os.path.join('C:/Users/jajohns/Files/base_data')
    
        # Define where the GEMPACK solver/license is installed
    p.gempack_utils_dir = "C://GP" 
    
    ### ------------- EVERYTHING BELOW IS DEFINED with RELATIVE PATHS -----------
    # define a list of years from 2018 to 2050
    p.years = list(range(2018, 2051)) 
    # p.years = list(range(2018, 2051, 5)) + [2050] # Every 5 years but add the same terminal 2050
    # p.years = list(range(2018, 2051)) # Exclusive of base_year
    # p.years = [2018, 2019] # Exclusive of base_year
    p.base_year = 2017
    p.key_base_year = 2017 # TODOO In seals we have a key base year and there are multiple p.base_years = []. Generalize this and make it so that base_year is singluar but p.base_years is renamed p.prior_years
    
    # Define wihch CGE release to use
    p.cge_model_release_string = 'gtapv7-aez-rd'
    
    # Point to the numeraire run of the model. In addition to showing that the 
    # base model works, this also generates files that can be used in subsequent
    # scenarios, specifically shockv7.har
    p.template_bau_oldschool_cmf_path = os.path.join(p.base_data_dir, 'gtappy', 'cge_releases', p.cge_model_release_string, 'cmf', 'gtapv7-aez-rd_bau.cmf')
    p.template_bau_es_cmf_path = os.path.join(p.base_data_dir, 'gtappy', 'cge_releases', p.cge_model_release_string, 'cmf', 'gtapv7-aez-rd_bau_es.cmf')
    
    # Define which aggregations will be used when GTAP is run.    
    fully_disaggregated_label = '65x141'
    shareable_and_fast_label = '10x10'  
    old_aez_cwon = 'v11-s26-r50_20231103'  
    cwon_aggregation_label = 'v11-s26-r50'
    
    
    
    p.aggregation_labels = [cwon_aggregation_label]    
    # p.aggregation_labels = [fully_disaggregated_label]
    # p.aggregation_labels = [shareable_and_fast_label, fully_disaggregated_label] # For use will non-test-set    

    # Define which scenarios (shocks) will be run
    # This example uses 1%, 2% and 3% increases in agricultural productivity.
    p.experiment_labels = ['bau',]
    # p.experiment_labels = ['bau', 'bau_es', ]
    # p.experiment_labels = ['GTAPv7-aez', 'TarElim', 'TarElimProd', ]
    
    # NOTGE GTAPv7-aez is provided by default with each new model. It also tests if Hod-1 in prices.
    
    # Generate a nested dictionary for all permutations of aggregations and experiments. 
    # This will set xsets, xsubsets, and shocks attributes of the ProjectFlow object p.
    gtappy_utils.set_attributes_based_on_aggregation_and_experiments(p, p.aggregation_labels, p.experiment_labels)
    
    
    p.custom_gtap_executable_filename = None # TODO
    p.cge_model_dir = os.path.join(p.base_data_dir, 'gtappy', 'cge_releases', p.cge_model_release_string)
    p.cge_executable_path = os.path.join(p.cge_model_dir, 'mod', p.cge_model_release_string + '.exe')
    p.cge_data_dir = os.path.join(p.cge_model_dir, 'data') # Note I just changed this to NOT have the aggregation in it. will need to be fixed for regular gtappy    



    
    ###------- Write the unique information that defines how each scenario's CMF is different.

    # Define AGGREGATION specific sets
    p.xsets['v11-s26-r50'] = []
    p.xsubsets['v11-s26-r50'] = []
    
 
    # Define SCENARIO specific information
    p.shocks['v11-s26-r50']['bau_ssp2'] = [
        'swap qe(\"capital\",REG) = capadd(REG);'
        'swap afelabreg = qgdppcfisher;'
        'swap qesf = qesfsupply;'
        'shock del_unity = 1;',
        'shock qgdppcfisher = file <p1>\BaseScen.har header \"OGP2\" slice "<p5>";'
        'shock pop = file <p1>\BaseScen.har header "POP2" slice "<p5>";',
        'shock qe(ENDWL,REG) = file <p1>\BaseScen.har header \"LAB2\" slice \"<p5>\";'
    ]
    p.shocks['v11-s26-r50']['bau_ssp3'] = [
        'swap qe(\"capital\",REG) = capadd(REG);'
        'swap afelabreg = qgdppcfisher;'
        'swap qesf = qesfsupply;'
        'shock del_unity = 1;',
        'shock qgdppcfisher = file <p1>\BaseScen.har header \"OGP2\" slice "<p5>";'
        'shock pop = file <p1>\BaseScen.har header "POP3" slice "<p5>";',
        'shock qe(ENDWL,REG) = file <p1>\BaseScen.har header \"LAB2\" slice \"<p5>\";'
    ]
    p.shocks['v11-s26-r50']['bau_ssp2_with_fire'] = [
        'swap qe(\"capital\",REG) = capadd(REG);'
        'swap afelabreg = qgdppcfisher;'
        'swap qesf = qesfsupply;'
        'shock del_unity = 1;',
        'shock qgdppcfisher = file <p1>\BaseScen.har header \"OGP2\" slice "<p5>";'
        'shock pop = file <p1>\BaseScen.har header "POP2" slice "<p5>";',
        'shock qe(ENDWL,REG) = file <p1>\NewHarFileWithPOP2AIR_Column.har header \"LAB2_fire\" slice \"<p5>\";'
    ]
#     ! (A1) Activate year-on-year capital accumulation equation
# swap qe("capital",REG) = capadd(REG);

# ! (A2) Endogenize labor productivity and exogenize GDP per capita
# swap afelabreg = qgdppcfisher;

# ! (A3) Upward sloping supply curve for sector-specific factor
# swap qesf = qesfsupply;

# !-----------------------------------
# ! (B) Impose baseline shocks
# !-----------------------------------
# ! (B1) Activate year-on-year capital accumulation mechanism  
# shock del_unity = 1;

# ! (B2) Real GDP per capita projections
# shock qgdppcfisher = file <p1>\BaseScen.har header "OGP2" slice "<p5>";

# ! (B3) Population growth projections 
# shock  pop = file <p1>\BaseScen.har header "POP2" slice "<p5>";   

# ! (B4) Labor force growth projections
# shock  qe(ENDWL,REG) = file <p1>\BaseScen.har header "LAB2" slice "<p5>";
    
    # Put any additional CMF commands here. These will overwrite things in the template cmf via a key = value string representation.
    # p.cmf_commands['v11-s26-r50']['bau'] = {'Steps': '6 12 18;', 'Method': 'euler;'}
    # p.cmf_commands['v11-s26-r50']['bau_es'] = {'Steps': '6 12 18;', 'Method': 'euler;'}
    


    # Define what cross-scenario comparisons to make
    p.headers_to_extract = ['pds', 'pfactor', 'qgdp', 'lcovercom', 'lcoveraez', 'p_ECONLAND', 'p_ECONLANDW', 'p_LANDCOVER_L']
    p.reg_vars_to_plot = ['qgdp', 'pfactor']
    

    
    

    p.mapping_10_path = hb.get_first_extant_path(os.path.join('gtappy', 'aggregation_mappings', 'new_mapfile_data', 'mapping_10_regs_to_4_and_3_inc_categories.csv'), [p.input_dir, p.base_data_dir])
    p.mapping_141_path = None # Not needed, cause no remapping is done here.


    p.mapping_paths = {}
    # p.mapping_paths[shareable_and_fast_label] = p.mapping_10_path
    # p.mapping_paths[fully_disaggregated_label] = p.mapping_141_path
    

    
    
    gtappy_initialize_project.run(p)

