import os
import hazelbean as hb
import food_systems_initialize_project
import gtap_functions
from gtap_invest import gtap_invest_integration_functions

if __name__ == '__main__':

        p = hb.ProjectFlow(r'../../projects/manuscript_v1')   
        
        
        fully_disaggregated_label = '65x141'
        shareable_and_fast_label = '10x10'
        
        p.aggregation_labels = [shareable_and_fast_label]
        # p.aggregation_labels = [fully_disaggregated_label]
        # p.aggregation_labels = [shareable_and_fast_label, fully_disaggregated_label] # For use will non-test-set
        
        p.experiment_labels = ['agpr20b', 'beef50', 'rtms5']
        
        p.cge_model_release_string = 'food_systems_2022_08_04'
        p.cge_model_dir = os.path.join(p.input_dir, p.cge_model_release_string)
        p.cge_data_dir = os.path.join(p.cge_model_dir, 'Model', 'gtapv7-cmd', 'data')
        
        p.mapping_10_path = os.path.join(p.input_dir, 'new_mapfile_data', 'mapping_10_regs_to_4_and_3_inc_categories.csv')
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
        
        

    