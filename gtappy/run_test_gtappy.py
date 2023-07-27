import os
import hazelbean as hb
import food_systems_initialize_project
import gtap_functions
from gtap_invest import gtap_invest_integration_functions


if __name__ == '__main__':

        p = hb.ProjectFlow(r'../../projects/great_degredation')   
        
        
        fully_disaggregated_label = '65x141'
        shareable_and_fast_label = '10x10'
        
        p.aggregation_labels = [shareable_and_fast_label]
        # p.aggregation_labels = [fully_disaggregated_label]
        # p.aggregation_labels = [shareable_and_fast_label, fully_disaggregated_label] # For use will non-test-set
        
        p.experiment_labels = ['BAU', 'RD', 'ED', 'SD']
        p.experiment_names = ['Business as usual', 'Reduced consumption', 'Economic development', 'Sustainable development']
        
        p.shock_labels = ['Pop', 'GDP', 'Yld']
        p.shock_names = ['Population', 'Gross domestic product - 2010 USD', 'Ag yield in kcal']
        
        p.shock_definitions = {}
        p.shock_definitions['reduced_consumption'] = ' shock [varname] by [value(s)]'
        
        p.cge_model_release_string = 'gtap_v7_2022_08_04'
        p.cge_model_dir = os.path.join(p.model_base_data_dir, 'cge_releases', p.cge_model_release_string)
        p.cge_data_dir = os.path.join(p.cge_model_dir, 'Model', 'gtapv7-cmd', 'data')
        
        p.mapping_10_path = os.path.join(p.model_base_data_dir, 'aggregation_mappings', 'new_mapfile_data', 'mapping_10_regs_to_4_and_3_inc_categories.csv')
        p.mapping_141_path = None
        p.mapping_paths = {}
        p.mapping_paths[shareable_and_fast_label] = p.mapping_10_path
        p.mapping_paths[fully_disaggregated_label] = p.mapping_141_path
        
        p.gempack_utils_dir = "C://GP"
        
        p.shocks_csv_path = os.path.join(p.input_dir, 'shocks.csv')

        p.run_type = None
        
        if p.run_type is None:        
            p.extract_from_basedata_har_task = p.add_task(gtap_functions.extract_from_basedata_har)                
            p.create_mapfile_har_task = p.add_task(gtap_functions.create_mapfile_har)                
            p.processed_shockfiles_task = p.add_task(gtap_functions.processed_shockfiles)                
            p.run_gtap_v7_executable_task = p.add_task(gtap_functions.run_gtap_v7_executable)
            p.gtap_v7_results_summary_task = p.add_task(gtap_functions.gtap_v7_results_summary)

        food_systems_initialize_project.run(p)
        
        

    