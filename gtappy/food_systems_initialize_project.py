import os
import hazelbean as hb
import gtap_functions

def run(p):

    # Configure the logger that captures all the information generated.
    p.L = hb.get_logger('test_run_gtap_invest')
    
    p.base_data_dir = os.path.join('C:\\', 'Users', 'jajohns', 'Files', 'Research', 'base_data')
    
    p.L.info('Created ProjectFlow object at ' + p.project_dir + '\n    from script ' + p.calling_script + '\n    with base_data set at ' + p.base_data_dir)



    p.execute()