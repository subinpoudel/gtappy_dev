import os
import hazelbean as hb
import gtappy_tasks

def run(p):

    # Configure the logger that captures all the information generated.
    p.L = hb.get_logger('test_run_gtap_invest')
    
    # build_task_tree_by_name(p, p.run_type)

    p.L.info('Created ProjectFlow object at ' + p.project_dir + '\n    from script ' + p.calling_script + '\n    with base_data set at ' + p.base_data_dir)
    p.script_dir = os.path.dirname(os.path.realpath(__file__))
    p.execute()

def build_task_tree_by_name(p, task_tree_name):
    full_task_tree_name = 'build_' + task_tree_name + '_task_tree'
    target_function = globals()[full_task_tree_name]
    print('Launching GTAPPY. Building task tree: ' + task_tree_name)

    target_function(p)

def build_extract_and_run_task_tree(p):
    
    p.base_data_as_csv_task = p.add_task(gtappy_tasks.base_data_as_csv)                
    p.mapfile_task = p.add_task(gtappy_tasks.mapfile)                
    p.gtap_runs_task = p.add_task(gtappy_tasks.gtap_runs)
    p.results_as_csv_task = p.add_task(gtappy_tasks.results_as_csv)
    p.visualization_task = p.add_task(gtappy_tasks.vizualization)

def build_process_aez_results_task_tree(p):
    p.process_aez_results_task = p.add_task(gtappy_tasks.process_aez_results)
    
def build_extract_and_run_aez_task_tree(p):
    
    p.base_data_as_csv_task = p.add_task(gtappy_tasks.base_data_as_csv)                
    # p.mapfile_task = p.add_task(gtappy_tasks.mapfile)                
    p.gtap_runs_task = p.add_task(gtappy_tasks.gtap_runs)
    p.results_as_csv_task = p.add_task(gtappy_tasks.results_as_csv)
    p.results_as_stacked_csv_task = p.add_task(gtappy_tasks.results_as_stacked_csv)
    p.single_year_variable_csvs_task = p.add_task(gtappy_tasks.single_year_variable_csvs)
    p.visualization_task = p.add_task(gtappy_tasks.vizualization)