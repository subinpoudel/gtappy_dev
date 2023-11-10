import hazelbean as hb
import os
import pandas as pd



def set_attributes_based_on_aggregation_and_experiments(p, aggregation_labels, experiment_labels):
    # Make a nested dictionary (tho this could be replaced by a scenarios.csv like in seals) for the aggregations and 
    # experiments to run.
    p.xsets = {}
    p.xsubsets = {}
    p.shocks = {}
    p.cmf_commands = {} #['gtapaez11-50']['TarElimProd']
    for aggregation_label in aggregation_labels:
        p.xsets[aggregation_label] = {}
        p.xsubsets[aggregation_label] = {}
        p.shocks[aggregation_label] = {}
        p.cmf_commands[aggregation_label] = {}
        for experiment_label in experiment_labels:
            p.xsets[aggregation_label][experiment_label] = {}
            p.xsubsets[aggregation_label][experiment_label] = {}
            p.shocks[aggregation_label][experiment_label] = {}
            p.cmf_commands[aggregation_label][experiment_label] = {}


def processed_shockfiles(p):
    
    if p.run_this:
        shocks_df = pd.read_csv(p.shocks_csv_path, index_col=0, header=[0, 1, 2], skipinitialspace=True)
        print('shocks_df', shocks_df)
        print('shocks_df index', shocks_df.index)
        print('shocks_df columns', shocks_df.columns)
 
        shocks2_df = shocks_df.unstack()
        
        print('shocks2_df', shocks2_df)
        # print('shocks2_df index', shocks2_df.index)
        # print('shocks2_df columns', shocks2_df.columns)      
        # 



def gtap_shockfile_to_df(input_path):
    """Reads a GTAP-style text file and returns it as a 1-col (plus 1 col index) DataFrame.
    Still must merge these in if considering multiple shockfiles."""
    with open(input_path) as rf:
        indices = []
        col = []
        for line in rf:
            if ';' in line:
                n_entries = int(line.split(' ')[0])
                indices = list(range((n_entries)))
            else:
                col.append(float(line))
        df = pd.DataFrame(index=indices, data=col, columns=[hb.file_root(input_path)])
    return df







