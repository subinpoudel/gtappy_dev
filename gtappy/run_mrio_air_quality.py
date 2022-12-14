"""
This script extracts from the MRIO GTAP Har specific CSVs.

For the Sumil, Chris Tessum air quality engagement, they wanted the MRIO extended GTAP database, processed for their use. 
"""


import os
import hazelbean as hb
import food_systems_initialize_project
from gtap_invest import gtap_invest_integration_functions

def extract_mrio_air_quality_csvs(p):
    gtap_invest_integration_functions.directory_of_hars_to_indexed_dfs(p.cge_data_dir, output_dir=p.cur_dir, produce_hars_from_csvs=True)
    


if __name__ == '__main__':

    p = hb.ProjectFlow(r'../../projects/mrio_air_quality')   
    p.L = hb.get_logger('test_run_gtap_invest')
    
    fully_disaggregated_label = '65x141'
    shareable_and_fast_label = '10x10'
    
    p.aggregation_labels = [shareable_and_fast_label]
    # p.aggregation_labels = [fully_disaggregated_label]
    # p.aggregation_labels = [shareable_and_fast_label, fully_disaggregated_label] # For use will non-test-set

    p.cge_data_dir = r"C:\GTPAg2\GTAP10A\GTAP-MRIO\2014"
    
    p.gempack_utils_dir = "C://GP"


    p.extract_from_basedata_har_task = p.add_task(extract_mrio_air_quality_csvs)                

    p.execute()
    
