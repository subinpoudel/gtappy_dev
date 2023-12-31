
import os
import hazelbean as hb
from gtappy import gtappy_file_io
from gtappy import gtappy_cmf_generation
from gtappy import gtappy_runner
from gtappy import gtappy_utils
import matplotlib.pyplot as plt

import multiprocessing

import pandas as pd
import numpy as np

def gtappy_base_data(p):
    pass 

def econ_results(p):
    pass 


    
def gadm_ingested(p):
    p.current_task_documentation = """
Input the GADM v 4.10 ADM0 layer and use shapely to simplify it so we can create multiple
resolutions of GADM.
    """    
    # hb.log(p.current_task_documentation)
    
    # Use ProjectFlow's get_path method to find the most "local" version of the file that exists and set that as the path
    # Note, gadm is the domain and i chose to keep with their notation of adm0 for this dimension, but it could have been more consistent as r263
    p.gadm_adm0_vector_input_path = p.get_path(os.path.join('cartographic', 'gadm', 'gadm_410_adm0.gpkg'), possible_dirs='default', copy_to_project=False)
    p.gadm_adm0_1sec_vector_path = p.get_path(os.path.join('cartographic', 'gadm', 'gadm_410_adm0_1sec.gpkg'))
    p.gadm_adm0_10sec_vector_path = p.get_path(os.path.join('cartographic', 'gadm', 'gadm_410_adm0_10sec.gpkg'))
    p.gadm_adm0_100sec_vector_path = p.get_path(os.path.join('cartographic', 'gadm', 'gadm_410_adm0_100sec.gpkg'))
    
    p.gadm_adm0_labels_path = p.get_path(os.path.join('cartographic', 'gadm', 'gadm_410_adm0_labels.csv'))

    p.gadm_adm0_raster_path = p.get_path(os.path.join('cartographic', 'gadm', 'gadm_410_adm0.tif'))
    

    # the local variable gadm is either a string (if unloaded) or the gdf (if loaded). This ensures we
    # only load it once.
    gadm = p.gadm_adm0_vector_input_path    
      
    
    # Note EE Spec validates by the existence of the LAST path generated.
    if not hb.path_exists(p.gadm_adm0_100sec_vector_path):
        ten_sec_size = 0.002777777777777777884
        sizes = {1: ten_sec_size * .1, 10: ten_sec_size, 100: 10 * ten_sec_size, } # Tried 1000: 100 * ten_sec_size but with the simplification algorithm used, it just was a jaggy mess and wasn't much smaller cause we're not dropping islands.
        template_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gadm_adm0.gpkg')) 
        
        for sec, deg in sizes.items():
            
            # Just load it on first pass
            if type(gadm) is str:
                gadm = hb.read_vector(p.gadm_adm0_vector_input_path)
            
            output_path = hb.suri(template_path, str(sec) + 'sec')        
            if not hb.path_exists(output_path):
                
                hb.log('Creating', output_path)            
                hb.simplify_geometry(gadm, output_path, tolerance=deg, preserve_topology=True, drop_below_tolerance_multiplier=None)
            
    # Create a table (no geometry) version that is quicker to load and will be used for later merges
    # Here is where we rename columns to be EE spec.
    if not hb.path_exists(p.gadm_adm0_labels_path):
        gdf = hb.read_vector(p.gadm_adm0_100sec_vector_path)
        df = gdf[[i for i in gdf.columns if i != 'geometry']]
        
        # # Sort based on value of label
        # df2 = df.sort_values('label')
        
        # Set pandas to show all rows
        pd.set_option('display.max_rows', 500)
        
        # print(df)
        # print(df2)
        
        
        # Add new column with integers starting at 1 and incrementing up
        df['id'] = np.arange(1, len(df) + 1).astype(np.int64)
        
        # Put the 'id' column first
        new_cols = ['id'] + [i for i in df.columns if i != 'id']
        df = df[new_cols]
        print(df)
    
        # Convert all floats to ints
        df = hb.df_convert_column_type(df, np.float64, np.int64, columns='all', ignore_nan=True, verbose=False)
        
        # Save
        hb.create_directories(p.gadm_adm0_labels_path)
        df.to_csv(p.gadm_adm0_labels_path, index=False)
        
    # Rasterize GADM IDs
    if not hb.path_exists(p.gadm_adm0_raster_path):
        
        # Here only, use the highest resolution gadm vector for accurate rasterization
        hb.log('Creating ' + p.gadm_adm0_raster_path + ' from ' + p.gadm_adm0_vector_input_path + ' using convert_polygons_to_id_raster.')
        hb.convert_polygons_to_id_raster(p.gadm_adm0_vector_input_path, p.gadm_adm0_raster_path, p.base_year_lulc_path,
                                id_column_label='id', data_type=5, ndv=-9999, all_touched=False, compress=True)
        
    # Make pyramidal
    overview_path = p.gadm_adm0_raster_path + '.ovr'
    if not hb.path_exists(overview_path):
        hb.log('Converting to global pyramid for ' + p.gadm_adm0_raster_path + ' using make_path_global_pyramid.')
        hb.make_path_global_pyramid(p.gadm_adm0_raster_path)
                
    

def gtap_aez_seals_correspondences(p):
    p.current_task_documentation = """
    Create correspondence CSVs from ISO3 countries to GTAPv11 160
    regions, and then to gtapaezv11 50ish regions, also put the classification
    for seals simplification and luh.  
    
    Note the careful naming where regions is used in file paths and for the df variable name
    but the column header is region
    """
    
    # TODOO Note that only the actual file strings are made EE spec. The variable names themselves still need to,
    
    # These are teh two input paths I receive from Erwin. They are unmodified except for adding honduras as separate.
    p.gtapv7_r251_r160_correspondence_input_path = os.path.join(p.base_data_dir, 'gtappy', 'aggregation_mappings', 'gtapv7_r251_r160_correspondence.xlsx') # Erwin's naming
    p.gtapv7_r251_s65_r50_s26_correspondence_input_path = os.path.join(p.base_data_dir, 'gtappy', 'aggregation_mappings', 'gtapv7_r251_s65_r50_s26_correspondence.xlsx') # Erwin's naming
    
    # Project level paths that initialize even without running this task
    
    # Usually I don't have to write out the non-correspondence files, but here I do because the full labels weren't in the inputs
    # so I write them to merge them in.
    # Also note that I chose not to have the word _regions or similar attached because this is a singular thing and its label is defined by r251
    p.gtapv7_r251_labels_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gtapv7_r251_labels.csv'))
    p.gtapv7_r160_labels_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gtapv7_r160_labels.csv'))
    p.gtapv7_r50_labels_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gtapv7_r50_labels.csv'))
    p.gtapv7_r251_r160_correspondence_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gtapv7_r251_r160_correspondence.csv'))
        
    # Activities and commodities are separate. Activiteis have 24 total, subset of commodities, which have 26
    p.gtapv7_s65_a24_correspondence_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gtapv7_s65_a24_correspondence.csv'))
    p.gtapv7_s65_c26_correspondence_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gtapv7_s65_c26_correspondence.csv'))
    p.gtapv7_r160_r50_correspondence_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gtapv7_r160_r50_correspondence.csv'))
    
    # Combine the above correspondences into a single file.
    p.gadm_r263_gtapv7_r251_correspondence_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gadm_r263_gtapv7_r251_correspondence.csv'))
    p.gtapv7_r251_r160_r50_correspondence = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gtapv7_r251_r160_r50_correspondence.csv'))
    p.gadm_r263_gtapv7_r251_r160_r50_correspondence_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gadm_r263_gtapv7_r251_r160_r50_correspondence.csv'))
    
    # Identical to the full previous one but handy for shorter naming, based on the cge release string
    p.gtapv7_aez_rd_correspondence_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gtapv7_aez_rd_correspondence.csv'))
    hb.create_directories(p.gtapv7_aez_rd_correspondence_path)
    
    if p.run_this:
        
        
        if not hb.path_all_exist([p.gtapv7_r251_labels_path, p.gtapv7_r160_labels_path, p.gtapv7_r251_r160_correspondence_path], verbose=True):
            
            # Process tine Legend worksheet in the input into two listings of the labels and ids from iso3 (as defined by gtap11 and gtap11 regions)
            gtap11_region_correspondence_input_legend = pd.read_excel(p.gtapv7_r251_r160_correspondence_input_path, sheet_name='Legend', header=1, index_col=None)
            
            # drop the Unnamed columns
            gtap11_region_correspondence_legend = gtap11_region_correspondence_input_legend[[i for i in gtap11_region_correspondence_input_legend.columns if 'Unnamed' not in i]]
                        
            # Rename columns to be match specification in EE   
            # Note that eg No. is used twice where the first is r160 and the second is r251. New input mapping should clarify                         
            gtap11_region_correspondence_legend = gtap11_region_correspondence_legend.rename(columns={'No.': 'gtapv7_r160_id', 'GTAP Region': 'gtapv7_r160_label', 'Description': 'gtapv7_r160_description', 'No..1': 'gtapv7_r251_id', 'Country (iso)': 'gtapv7_r251_label', 'Description.1': 'gtapv7_r251_description'})
            
            # Shuffle things around to match the EE spec
            gtap11_iso3_1 = gtap11_region_correspondence_legend[['gtapv7_r251_id', 'gtapv7_r251_label', 'gtapv7_r251_description']]
            gtap11_iso3_2 = gtap11_iso3_1.dropna()
            gtapv7_r251_naming = gtap11_iso3_2.rename(columns={'gtapv7_r251_description': 'gtapv7_r251_name'})
            gtapv7_r251_naming['gtapv7_r251_description'] = gtapv7_r251_naming['gtapv7_r251_name'] # Set as a copy because the description in the input is actually a name but we still need an entry for the description

            # Rename Reunion
            gtapv7_r251_naming.loc[gtapv7_r251_naming['gtapv7_r251_label'] == 'reu', 'gtapv7_r251_name'] = 'Reunion'
            gtapv7_r251_naming.loc[gtapv7_r251_naming['gtapv7_r251_label'] == 'reu', 'gtapv7_r251_description'] = 'Réunion'
            gtapv7_r251_naming.loc[gtapv7_r251_naming['gtapv7_r251_label'] == 'ala', 'gtapv7_r251_name'] = 'Aland'
            gtapv7_r251_naming.loc[gtapv7_r251_naming['gtapv7_r251_label'] == 'ala', 'gtapv7_r251_description'] = 'Åland'            
            gtapv7_r251_naming.loc[gtapv7_r251_naming['gtapv7_r251_label'] == 'blm', 'gtapv7_r251_name'] = 'Saint Barthelemy'
            gtapv7_r251_naming.loc[gtapv7_r251_naming['gtapv7_r251_label'] == 'blm', 'gtapv7_r251_description'] = 'Saint Barthélemy'            
            gtapv7_r251_naming.loc[gtapv7_r251_naming['gtapv7_r251_label'] == 'civ', 'gtapv7_r251_name'] = 'Cote d\'lvoire'
            gtapv7_r251_naming.loc[gtapv7_r251_naming['gtapv7_r251_label'] == 'civ', 'gtapv7_r251_description'] = 'Côte d\'lvoire'            
            gtapv7_r251_naming.loc[gtapv7_r251_naming['gtapv7_r251_label'] == 'cuw', 'gtapv7_r251_name'] = 'Curacao'
            gtapv7_r251_naming.loc[gtapv7_r251_naming['gtapv7_r251_label'] == 'cuw', 'gtapv7_r251_description'] = 'Curaçao'            
            gtapv7_r251_naming.loc[gtapv7_r251_naming['gtapv7_r251_label'] == 'mex', 'gtapv7_r251_name'] = 'Mexico'
            gtapv7_r251_naming.loc[gtapv7_r251_naming['gtapv7_r251_label'] == 'mex', 'gtapv7_r251_description'] = 'Mexico'            
            gtapv7_r251_naming.loc[gtapv7_r251_naming['gtapv7_r251_label'] == 'stp', 'gtapv7_r251_name'] = 'Sao Tome and Principe'
            gtapv7_r251_naming.loc[gtapv7_r251_naming['gtapv7_r251_label'] == 'stp', 'gtapv7_r251_description'] = 'São Tomé and Príncipe'            
                        
            # Convert all floats to ints
            gtapv7_r251_naming = hb.df_convert_column_type(gtapv7_r251_naming, np.float64, np.int64, columns='all', ignore_nan=True, verbose=False)

            # Write it to a csv while dropping the index
            gtapv7_r251_naming.to_csv(p.gtapv7_r251_labels_path, index=False)
            
            # Shuffle things around to match the EE spec
            gtap11_regions_1 = gtap11_region_correspondence_legend[['gtapv7_r160_id', 'gtapv7_r160_label', 'gtapv7_r160_description']].copy()
            gtap11_regions = gtap11_regions_1.copy().dropna()
            gtap11_regions['gtapv7_r160_name'] = gtap11_regions['gtapv7_r160_description'].copy()
            # gtap11_regions = gtap11_regions_2.rename(columns={'gtapv7_r160_description': 'gtapv7_r160_name'})
            # gtap11_regions['gtapv7_r160_description'] = gtap11_regions['gtapv7_r160_name'] # Set as a copy because the description in the input is actually a name but we still need an entry for the description
            
            # Note for input No., it was interpretted as a float. fix that here.
            gtap11_regions['gtapv7_r160_id'] = gtap11_regions['gtapv7_r160_id'].astype(np.int64)
            
            # show all pandas rows

            gtap11_regions.loc[gtap11_regions['gtapv7_r160_label'] == 'civ', 'gtapv7_r160_name'] = 'Cote d\'lvoire'
            gtap11_regions.loc[gtap11_regions['gtapv7_r160_label'] == 'civ', 'gtapv7_r160_description'] = 'Côte d\'lvoire'
            gtap11_regions.loc[gtap11_regions['gtapv7_r160_label'] == 'tur', 'gtapv7_r160_name'] = 'Turkey'        
            gtap11_regions.loc[gtap11_regions['gtapv7_r160_label'] == 'tur', 'gtapv7_r160_description'] = 'Türkiye'        

            # Convert all floats to ints                    
            gtap11_regions = hb.df_convert_column_type(gtap11_regions, np.float64, np.int64, columns='all', ignore_nan=True, verbose=False)

            # Order the cols
            gtap11_regions = gtap11_regions[['gtapv7_r160_id', 'gtapv7_r160_label', 'gtapv7_r160_name', 'gtapv7_r160_description']]
        
            # Write it to a csv while dropping the index
            gtap11_regions.to_csv(p.gtapv7_r160_labels_path, index=False)
            
            # Process the MapCTRY2GREG worksheet in the input
            gtap11_region_correspondence_input_map = pd.read_excel(p.gtapv7_r251_r160_correspondence_input_path, sheet_name='MapCTRY2GREG', header=1, index_col=None)
            df = gtap11_region_correspondence_input_map[[i for i in gtap11_region_correspondence_input_map.columns if 'Unnamed' not in i]]
            df2 = df.rename(columns={'No.': 'gtapv7_r251_id', 'Country (iso)': 'gtapv7_r251_label', 'Name': 'gtapv7_r251_name', 'GTAP region': 'gtapv7_r160_label', 'Name.1': 'gtapv7_r160_name'})
            
            # Mergine in values from gtap11_region_ids
            gtap11_regions = pd.read_csv(p.gtapv7_r160_labels_path)
            gtap11_regions = gtap11_regions.drop(columns=['gtapv7_r160_name'])
            df2a = hb.df_merge(df2, gtap11_regions, left_on='gtapv7_r160_label', right_on='gtapv7_r160_label', verbose=False)
            df2a['gtapv7_r251_description'] = df2a['gtapv7_r251_name']
            df2a['gtapv7_r160_description'] = df2a['gtapv7_r160_name']
            
            pd.set_option('display.max_rows', 500)
            print(gtap11_regions)
            # Reorder
            df4 = df2a[['gtapv7_r251_id', 'gtapv7_r160_id', 'gtapv7_r251_label', 'gtapv7_r160_label', 'gtapv7_r251_name', 'gtapv7_r160_name', 'gtapv7_r251_description', 'gtapv7_r160_description']] 
                        
            gtap11_iso3_gtap11_region_correspondence = df4.sort_values('gtapv7_r160_id') # Sort back to the original order per the EE spec

            # Convert all floats to ints
            gtap11_iso3_gtap11_region_correspondence = hb.df_convert_column_type(gtap11_iso3_gtap11_region_correspondence, np.float64, np.int64, columns='all', ignore_nan=True, verbose=False)
            
            
            gtap11_iso3_gtap11_region_correspondence.loc[gtap11_iso3_gtap11_region_correspondence['gtapv7_r160_label'] == 'civ', 'gtapv7_r160_name'] = 'Cote d\'lvoire'
            gtap11_iso3_gtap11_region_correspondence.loc[gtap11_iso3_gtap11_region_correspondence['gtapv7_r160_label'] == 'civ', 'gtapv7_r160_description'] = 'Côte d\'lvoire'
            gtap11_iso3_gtap11_region_correspondence.loc[gtap11_iso3_gtap11_region_correspondence['gtapv7_r160_label'] == 'tur', 'gtapv7_r160_name'] = 'Turkey'        
            gtap11_iso3_gtap11_region_correspondence.loc[gtap11_iso3_gtap11_region_correspondence['gtapv7_r160_label'] == 'tur', 'gtapv7_r160_description'] = 'Türkiye'        
            gtap11_iso3_gtap11_region_correspondence.loc[gtap11_iso3_gtap11_region_correspondence['gtapv7_r251_label'] == 'civ', 'gtapv7_r251_name'] = 'Cote d\'lvoire'
            gtap11_iso3_gtap11_region_correspondence.loc[gtap11_iso3_gtap11_region_correspondence['gtapv7_r251_label'] == 'civ', 'gtapv7_r251_description'] = 'Côte d\'lvoire'
            gtap11_iso3_gtap11_region_correspondence.loc[gtap11_iso3_gtap11_region_correspondence['gtapv7_r251_label'] == 'tur', 'gtapv7_r251_name'] = 'Turkey'        
            gtap11_iso3_gtap11_region_correspondence.loc[gtap11_iso3_gtap11_region_correspondence['gtapv7_r251_label'] == 'tur', 'gtapv7_r251_description'] = 'Türkiye'        

            
            # Write it to a csv while dropping the index
            gtap11_iso3_gtap11_region_correspondence.to_csv(p.gtapv7_r251_r160_correspondence_path, index=False)
        
        if not hb.path_exists(p.gadm_r263_gtapv7_r251_correspondence_path):
            
            # Load the two tables
            gadm = pd.read_csv(p.gadm_adm0_labels_path)
            gtapv7_r251_naming = pd.read_csv(p.gtapv7_r251_labels_path)
                        
            # Make gadm lowercase
            gadm['GID_0'] = gadm['GID_0'].str.lower()

            # TODOO Figure out how to make this a HB level choice.
            pd.set_option('display.max_column', 33)
            pd.set_option('expand_frame_repr', False)
            

            gadm = gadm.rename(columns={'id': 'gadm_r263_id', 'GID_0': 'gadm_r263_label', 'COUNTRY': 'gadm_r263_name'})
            gadm['gadm_r263_description'] = gadm['gadm_r263_name'] 
            
            # Fix reunion naming
            gadm.loc[gadm['gadm_r263_label'] == 'reu', 'gadm_r263_name'] = 'Reunion'
            gadm.loc[gadm['gadm_r263_label'] == 'reu', 'gadm_r263_description'] = 'Réunion'            
            gadm.loc[gadm['gadm_r263_label'] == 'ala', 'gadm_r263_name'] = 'Aland'
            gadm.loc[gadm['gadm_r263_label'] == 'ala', 'gadm_r263_description'] = 'Åland'            
            gadm.loc[gadm['gadm_r263_label'] == 'blm', 'gadm_r263_name'] = 'Saint Barthelemy'
            gadm.loc[gadm['gadm_r263_label'] == 'blm', 'gadm_r263_description'] = 'Saint Barthélemy'            
            gadm.loc[gadm['gadm_r263_label'] == 'civ', 'gadm_r263_name'] = 'Cote d\'lvoire'
            gadm.loc[gadm['gadm_r263_label'] == 'civ', 'gadm_r263_description'] = 'Côte d\'lvoire'            
            gadm.loc[gadm['gadm_r263_label'] == 'cuw', 'gadm_r263_name'] = 'Curacao'
            gadm.loc[gadm['gadm_r263_label'] == 'cuw', 'gadm_r263_description'] = 'Curaçao'            
            gadm.loc[gadm['gadm_r263_label'] == 'mex', 'gadm_r263_name'] = 'Mexico'
            gadm.loc[gadm['gadm_r263_label'] == 'mex', 'gadm_r263_description'] = 'Mexico'            
            gadm.loc[gadm['gadm_r263_label'] == 'stp', 'gadm_r263_name'] = 'Sao Tome and Principe'
            gadm.loc[gadm['gadm_r263_label'] == 'stp', 'gadm_r263_description'] = 'São Tomé and Príncipe'            
            
            
            gadm_r263_labels_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gadm_r263_labels.csv'))
            gadm.to_csv(gadm_r263_labels_path, index=False)
            
            # Do an outer merge
            df = hb.df_merge(gadm, gtapv7_r251_naming, left_on='gadm_r263_label', right_on='gtapv7_r251_label', verbose=False)
            
            
            # fill in missing that ARE in r251 but not in r263] 
            df.loc[df['gtapv7_r251_label'] == 'hkg', 'gadm_r263_id'] = 44
            df.loc[df['gtapv7_r251_label'] == 'hkg', 'gadm_r263_label'] = 'chn'
            df.loc[df['gtapv7_r251_label'] == 'hkg', 'gadm_r263_name'] = 'China'
            df.loc[df['gtapv7_r251_label'] == 'hkg', 'gadm_r263_description']= 'China'
            
            #Macao
            df.loc[df['gtapv7_r251_label'] == 'mac', 'gadm_r263_id'] = 44
            df.loc[df['gtapv7_r251_label'] == 'mac', 'gadm_r263_label'] = 'chn'
            df.loc[df['gtapv7_r251_label'] == 'mac', 'gadm_r263_name'] = 'China'
            df.loc[df['gtapv7_r251_label'] == 'mac', 'gadm_r263_description']= 'China'

            # Kosovo (both have it, but have different iso3) and GTAPv7 has a weird typo? Override with gadm.
            df = df[df['gtapv7_r251_label'] != 'xkx']
            df.loc[df['gadm_r263_label'] == 'xko', 'gtapv7_r251_label'] = 'xkx'


            
            # Sark fix (both have it, but have different iso3)
            df.loc[df['gtapv7_r251_label'] == 'sar', 'gadm_r263_id'] = 192
            df.loc[df['gtapv7_r251_label'] == 'sar', 'gadm_r263_label'] = 'ggy'
            df.loc[df['gtapv7_r251_label'] == 'sar', 'gadm_r263_name'] = 'Guernsey'
            df.loc[df['gtapv7_r251_label'] == 'sar', 'gadm_r263_description']= 'Guernsey'

            hb.log('Adding fixes to GADM\n\n', df, level=100)
            
            # Clean            
            new_cols = ['gadm_r263_id', 'gtapv7_r251_id', 'gadm_r263_label', 'gtapv7_r251_label', 'gadm_r263_name', 'gtapv7_r251_name', 'gadm_r263_description', 'gtapv7_r251_description']
            df = df[new_cols]     
            
            # Drop any row with a missing value in id, label or name
            # df = df.dropna(subset=['gadm_r263_id', 'gadm_r263_label', 'gadm_r263_name', 'gtapv7_r251_id', 'gtapv7_r251_label', 'gtapv7_r251_name'])    
            # Drop the polygon of the caspian sea (XCA)
            # Drop a row where GID_0 = XCA
            # merged_df = merged_df[merged_df['GID_0'] != 'XCA']
            
            # Choose whichever adjacent or nearby GTAP country is biggest among the people claiming sogerignty to assign. no politics implied.
            # df.loc[df['gtapv7_r251_label'] == 'hmd', 'gadm_r263_id'] = 97 
            # df.loc[df['gtapv7_r251_label'] == 'hmd', 'gadm_r263_label'] = 'hmd'
            # df.loc[df['gtapv7_r251_label'] == 'hmd', 'gadm_r263_name'] = 'Heard Island and McDonald Island'
            # df.loc[df['gtapv7_r251_label'] == 'hmd', 'gadm_r263_description'] = 'Heard Island and McDonald Island' 
            
            # Increase the number of rows printed by pandas when you print a df
            pd.set_option('display.max_rows', 500)
            
            
            hb.log('hmd\n', df, level=100)
            replacements = {}
            replacements['HMD'] = [98, 'HMD', 'Heard Island and McDonald Island', 'Heard Island and McDonald Island']
            replacements['XAD'] = [80, 'GBR', 'United Kingdom', 'United Kingdom of Great Britain and Northern Ireland']
            replacements['XCL'] = [76, 'FRA', 'France', 'France']
            replacements['XKO'] = [206, 'XKX', 'Serbia', 'Serbia']
            replacements['XPI'] = [44, 'CHN', 'China', 'China']
            replacements['XSP'] = [44, 'CHN', 'China', 'China']
            replacements['Z01'] = [105, 'IND', 'India', 'India']
            replacements['Z02'] = [44, 'CHN', 'China', 'China']
            replacements['Z03'] = [44, 'CHN', 'China', 'China']
            replacements['Z04'] = [105, 'IND', 'India', 'India']
            replacements['Z05'] = [105, 'IND', 'India', 'India']
            replacements['Z06'] = [173, 'PAK', 'Pakistan', 'Pakistan']
            replacements['Z07'] = [105, 'IND', 'India', 'India']
            replacements['Z08'] = [44, 'CHN', 'China', 'China']
            replacements['Z09'] = [105, 'IND', 'India', 'India']
            replacements['ZNC'] = [58, 'CYP', 'Cyprus', 'Cyprus']
            
            # Make the same list as above but in lowercase
            replacements_lower = {}
            for key, value in replacements.items():
                replacements_lower[key.lower()] = [value[0], value[1].lower(), value[2], value[3]] 
            
            # Use replacements to fill missing values in the merged_df
            for key, value in replacements_lower.items():

                df.loc[df['gadm_r263_label'] == key, 'gtapv7_r251_id'] = value[0]          
                df.loc[df['gadm_r263_label'] == key, 'gtapv7_r251_label'] = value[1]           
                df.loc[df['gadm_r263_label'] == key, 'gtapv7_r251_name'] = value[2]            
                df.loc[df['gadm_r263_label'] == key, 'gtapv7_r251_description'] = value[3]            



            hb.log('AFTER FIX\n\n', df, level=100)
            # Convert all floats to ints
            df2 = hb.df_convert_column_type(df, np.float64, np.int64, columns='all', ignore_nan=True, verbose=False)
            
            df2 = df2[df2['gadm_r263_label'] != 'xca']
        
            # df2.loc[:, 'gadm_r263_description'] = df2['gadm_r263_name']

            df2.to_csv(p.gadm_r263_gtapv7_r251_correspondence_path, index=False)
            
                   
           
        if not hb.path_all_exist([p.gtapv7_s65_a24_correspondence_path, p.gtapv7_s65_c26_correspondence_path, p.gtapv7_r160_r50_correspondence_path]):
            
            # Read the common input XLSX worksheet for sectors and activities
            gtap11_sectors_gtapaez11_sectors_correspondence_input_sectors = pd.read_excel(p.gtapv7_r251_s65_r50_s26_correspondence_input_path, sheet_name='Sectors', header=1, index_col=None)
            
            # Also get the Legends worksheet
            gtap11_sectors_gtapaez11_sectors_correspondence_input_legend = pd.read_excel(p.gtapv7_r251_s65_r50_s26_correspondence_input_path, sheet_name='Legend', header=1, index_col=None)
            
            
            # Split Activities and Commodities (ACT, COMM)
            # SELECT just activities
            df = gtap11_sectors_gtapaez11_sectors_correspondence_input_sectors[['No.', 'Sector', 'Aggregate Activity', 'Description']]
            rename_dict = {
                            'No.': 'gtapv7_s65_id', 
                            'Sector': 'gtapv7_s65_label',
                            'Aggregate Activity': 'gtapv7_a24_label',
                            'Description': 'gtapv7_a24_name',
                            }            
            df2 = df.rename(columns=rename_dict)
            # Add empty columns
            # 
            # # Get the name and description from the legend
            df_s65_sectors_legend = gtap11_sectors_gtapaez11_sectors_correspondence_input_legend[['No..1', 'Sector', 'Description']]
            rename_dict = {
                'No..1': 'gtapv7_s65_id', 
                'Sector': 'gtapv7_s65_label',
                'Description': 'gtapv7_s65_name',
                }      
            df_s65_sectors_legend = df_s65_sectors_legend.rename(columns=rename_dict)            
            df_s65_sectors_legend['gtapv7_s65_description'] = df_s65_sectors_legend['gtapv7_s65_name'] # Set as a copy because the description in the input is actually a name but we still need an entry for the description
            df_s65_sector_labels_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gtapv7_s65_labels.csv'))
            # Drop empty rows
            df_s65_sectors_legend = df_s65_sectors_legend.dropna(subset=['gtapv7_s65_id', 'gtapv7_s65_label', 'gtapv7_s65_name', 'gtapv7_s65_description'])
            
            df_s65_sectors_legend = hb.df_convert_column_type(df_s65_sectors_legend, np.float64, np.int64, columns='all', ignore_nan=True, verbose=False)

            
            df_s65_sectors_legend.to_csv(df_s65_sector_labels_path, index=False)            
                        
                        
            df_a24_activities_legend = gtap11_sectors_gtapaez11_sectors_correspondence_input_legend[['No..3', 'Activity', 'Aggregate activity description']]
            rename_dict = {
                'No..3': 'gtapv7_a24_id', 
                'Activity': 'gtapv7_a24_label',
                'Aggregate activity description': 'gtapv7_a24_name',
                }      
            df_a24_activities_legend = df_a24_activities_legend.rename(columns=rename_dict)            
            df_a24_activities_legend['gtapv7_a24_description'] = df_a24_activities_legend['gtapv7_a24_name'] # Set as a copy because the description in the input is actually a name but we still need an entry for the description
            

            df_a24_sector_labels_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gtapv7_a24_labels.csv'))
            # Drop empty rows
            
            df_a24_activities_legend = df_a24_activities_legend.dropna(subset=['gtapv7_a24_id', 'gtapv7_a24_label', 'gtapv7_a24_name', 'gtapv7_a24_description'])
            df_a24_activities_legend = hb.df_convert_column_type(df_a24_activities_legend, np.float64, np.int64, columns='all', ignore_nan=True, verbose=False)
            df_a24_activities_legend.to_csv(df_a24_sector_labels_path, index=False)     
            
            df_c26_activities_legend = gtap11_sectors_gtapaez11_sectors_correspondence_input_legend[['No..4', 'Commodity', 'Aggregate commodity description']]
            rename_dict = {
                'No..4': 'gtapv7_c26_id', 
                'Commodity': 'gtapv7_c26_label',
                'Aggregate commodity description': 'gtapv7_c26_name',
                }      
            df_c26_activities_legend = df_c26_activities_legend.rename(columns=rename_dict)            
            df_c26_activities_legend['gtapv7_c26_description'] = df_c26_activities_legend['gtapv7_c26_name'] # Set as a copy because the description in the input is actually a name but we still need an entry for the description
            df_c26_sector_labels_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', 'gtapv7_c26_labels.csv'))
            df_c26_activities_legend = df_c26_activities_legend.dropna(subset=['gtapv7_c26_id', 'gtapv7_c26_label', 'gtapv7_c26_name', 'gtapv7_c26_description'])
            
            df_c26_activities_legend = hb.df_convert_column_type(df_c26_activities_legend, np.float64, np.int64, columns='all', ignore_nan=True, verbose=False)

            df_c26_activities_legend.to_csv(df_c26_sector_labels_path, index=False)    
            
            df2['gtapv7_s65_name'] = ''
            df2['gtapv7_a24_description'] = ''
            df3 = df2.sort_values('gtapv7_a24_label') # First sort it by the gtapaez11_activity_label to assign an order ID to it
            grouped = df3.groupby('gtapv7_a24_label')
            df3['gtapv7_a24_id'] = grouped.ngroup() + 1
            df3['gtapv7_s65_description'] = df3['gtapv7_s65_name'] # Set as a copy because the description in the input is actually a name but we still need an entry for the description
            df4 = df3[['gtapv7_s65_id', 'gtapv7_a24_id', 'gtapv7_s65_label', 'gtapv7_a24_label', 'gtapv7_s65_name', 'gtapv7_a24_name', 'gtapv7_s65_description', 'gtapv7_a24_description']] 
            gtapv7_s65_a24_correspondence = df4.sort_values('gtapv7_a24_id') # Sort back to the original order per the EE spec
            
            # Convert all floats to ints
            gtapv7_s65_a24_correspondence = hb.df_convert_column_type(gtapv7_s65_a24_correspondence, np.float64, np.int64, columns='all', ignore_nan=True, verbose=False)
                                
            # Merge back in descriptions
            gtapv7_s65_a24_correspondence = gtapv7_s65_a24_correspondence.drop(columns=['gtapv7_s65_name', 'gtapv7_s65_description', 'gtapv7_s65_id'])
            gtapv7_s65_a24_correspondence = hb.df_merge(gtapv7_s65_a24_correspondence, df_s65_sectors_legend, left_on='gtapv7_s65_label', right_on='gtapv7_s65_label', verbose=False)
            gtapv7_s65_a24_correspondence['gtapv7_a24_description'] = gtapv7_s65_a24_correspondence['gtapv7_a24_name']
            # Reorder so it matches the EE spec
            gtapv7_s65_a24_correspondence = gtapv7_s65_a24_correspondence[['gtapv7_s65_id', 'gtapv7_a24_id', 'gtapv7_s65_label', 'gtapv7_a24_label', 'gtapv7_s65_name', 'gtapv7_a24_name', 'gtapv7_s65_description', 'gtapv7_a24_description']]
            
            
            # Write it to a csv while dropping the index
            gtapv7_s65_a24_correspondence.to_csv(p.gtapv7_s65_a24_correspondence_path, index=False)
            
            # SELECT just commodities
            df = gtap11_sectors_gtapaez11_sectors_correspondence_input_sectors[['No..1', 'Sector.1', 'Aggregate Commodity', 'Description.1']]
            
            rename_dict = {
                            'No..1': 'gtapv7_s65_id',
                            'Sector.1': 'gtapv7_s65_label',
                            'Aggregate Commodity': 'gtapv7_c26_label',
                            'Description.1': 'gtapv7_c26_name',
                            }
            df2 = df.rename(columns=rename_dict)
            # Add empty columns
            df2['gtapv7_s65_name'] = ''
            df2['gtapv7_c26_description'] = ''
            df3 = df2.sort_values('gtapv7_c26_label') # First sort it by the gtapaez11_commodity_label to assign an order ID to it
            grouped = df3.groupby('gtapv7_c26_label')
            df3['gtapv7_c26_id'] = grouped.ngroup() + 1
            df3['gtapv7_c64_description'] = df3['gtapv7_s65_name'] # Set as a copy because the description in the input is actually a name but we still need an entry for the description

            df4 = df3[['gtapv7_s65_id', 'gtapv7_c26_id', 'gtapv7_s65_label', 'gtapv7_c26_label', 'gtapv7_s65_name', 'gtapv7_c26_name', 'gtapv7_c64_description', 'gtapv7_c26_description']] 
            gtapv7_s65_c26_correspondence = df4.sort_values('gtapv7_s65_id') # Sort back to the original order per the EE spec

            # Convert all floats to ints
            gtapv7_s65_c26_correspondence = hb.df_convert_column_type(gtapv7_s65_c26_correspondence, np.float64, np.int64, columns='all', ignore_nan=True, verbose=False)

            # Merge back in descriptions ['gtapv7_s65_name', 'gtapv7_s65_description', 'gtapv7_s65_id'] 
            gtapv7_s65_c26_correspondence = gtapv7_s65_c26_correspondence.drop(columns=['gtapv7_s65_name', 'gtapv7_s65_id'])
            gtapv7_s65_c26_correspondence = hb.df_merge(gtapv7_s65_c26_correspondence, df_s65_sectors_legend, left_on='gtapv7_s65_label', right_on='gtapv7_s65_label', verbose=False)
            gtapv7_s65_c26_correspondence['gtapv7_c26_description'] = gtapv7_s65_c26_correspondence['gtapv7_c26_name']
            
            # Reorder so it matches the EE spec
            gtapv7_s65_c26_correspondence = gtapv7_s65_c26_correspondence[['gtapv7_s65_id', 'gtapv7_c26_id', 'gtapv7_s65_label', 'gtapv7_c26_label', 'gtapv7_s65_name', 'gtapv7_c26_name', 'gtapv7_s65_description', 'gtapv7_c26_description']]
            
            

            # Write it to a csv while dropping the index
            gtapv7_s65_c26_correspondence.to_csv(p.gtapv7_s65_c26_correspondence_path, index=False)
            # df = gtapv7_s65_c26_correspondence[[i for i in gtapv7_s65_c26_correspondence.columns if 'Unnamed' not in i]]            
            
            # LOAD the Region worksheet
            gtap11_regions_gtapaez11_regions_correspondence_input_region = pd.read_excel(p.gtapv7_r251_s65_r50_s26_correspondence_input_path, sheet_name='Region', header=1, index_col=None)
            rename_dict = {
                            'No.': 'gtapv7_r160_id', 
                            'Region': 'gtapv7_r160_label',
                            'Aggregate Region': 'gtapv7_r50_label',
                            'Aggregate Country/Region name': 'gtapv7_r50_name',
                            }          
            
            df2 = gtap11_regions_gtapaez11_regions_correspondence_input_region.rename(columns=rename_dict)

            df3 = df2.sort_values('gtapv7_r50_label') # First sort it by the gtapaez11_region_label to assign an order ID to it
            grouped = df3.groupby('gtapv7_r50_label')
            df3['gtapv7_r50_id'] = grouped.ngroup() + 1
            
            # Merge in the gtapv7_r160_name
            gtap11_regions = pd.read_csv(p.gtapv7_r160_labels_path)
            df3 = hb.df_merge(df3, gtap11_regions, left_on='gtapv7_r160_label', right_on='gtapv7_r160_label', verbose=False)
            
            df3['gtapv7_r160_description'] = df3['gtapv7_r160_name'].copy() # Set as a copy because the description in the input is actually a name but we still need an entry for the description
            df3['gtapv7_r50_description'] = df3['gtapv7_r50_name'].copy() # Set as a copy because the description in the input is actually a name but we still need an entry for the description
            df4 = df3[['gtapv7_r160_id', 'gtapv7_r50_id', 'gtapv7_r160_label', 'gtapv7_r50_label', 'gtapv7_r50_description', 'gtapv7_r160_name', 'gtapv7_r50_name', 'gtapv7_r160_description', 'gtapv7_r50_description']] 
            gtapv7_r160_r50_correspondence = df4.sort_values('gtapv7_r50_id') # Sort back to the original order per the EE spec
                        
            # Convert all floats to ints
            gtapv7_r160_r50_correspondence = hb.df_convert_column_type(gtapv7_r160_r50_correspondence, np.float64, np.int64, columns='all', ignore_nan=True, verbose=False)
                    
            gtapv7_r160_r50_correspondence.to_csv(p.gtapv7_r160_r50_correspondence_path, index=False)
            
       
        if not hb.path_exists(p.gadm_r263_gtapv7_r251_r160_r50_correspondence_path):
            # BNow combine gadm label, gtapiso3, gtapregion, and gtapaezregion.v
            gadm_gid0_gtap11_iso3_correspondence = pd.read_csv(p.gadm_r263_gtapv7_r251_correspondence_path)   
            gtap11_iso3_gtap11_regions_correspondence = pd.read_csv(p.gtapv7_r251_r160_correspondence_path)
            gtapv7_r160_r50_correspondence = pd.read_csv(p.gtapv7_r160_r50_correspondence_path)
            
            df = hb.df_merge(gadm_gid0_gtap11_iso3_correspondence, gtap11_iso3_gtap11_regions_correspondence, left_on='gtapv7_r251_label', right_on='gtapv7_r251_label', verbose=False)            
            df = hb.df_merge(df, gtapv7_r160_r50_correspondence, left_on='gtapv7_r160_label', right_on='gtapv7_r160_label', verbose=False)      
            df = df.copy()  
            # Convert all floats to ints
            df = hb.df_convert_column_type(df, np.float64, np.int64, columns='all', ignore_nan=True, verbose=False)
            
            hb.log('Before\n', df, level=100)                          

            df = hb.df_merge_two_columns_filling_missing(df, 'gtapv7_r251_id_x', 'gtapv7_r251_id_y', 'gtapv7_r251_id')
            df = hb.df_merge_two_columns_filling_missing(df, 'gtapv7_r160_id_x', 'gtapv7_r160_id_y', 'gtapv7_r160_id')
            df = hb.df_merge_two_columns_filling_missing(df, 'gtapv7_r251_name_x', 'gtapv7_r251_name_y', 'gtapv7_r251_name')
            df = hb.df_merge_two_columns_filling_missing(df, 'gtapv7_r251_description_x', 'gtapv7_r251_description_y', 'gtapv7_r251_description')
            df = hb.df_merge_two_columns_filling_missing(df, 'gtapv7_r160_description_x', 'gtapv7_r160_description_y', 'gtapv7_r160_description')
            df = hb.df_merge_two_columns_filling_missing(df, 'gtapv7_r160_name_x', 'gtapv7_r160_name_y', 'gtapv7_r160_name')
            hb.log('After\n', df, level=100) 
            # rename gadm_r263_id to gadm_r263_id
            df = df.rename(columns={'gadm_r263_id': 'gadm_r263_id'})
            df = df.rename(columns={'gadm_r263_label': 'gadm_r263_label'})
            df = df.rename(columns={'gadm_r263_name': 'gadm_r263_name'})
            df = df.rename(columns={'gadm_r263_description': 'gadm_r263_description'})
            
            # Merge back in gtapv7_r50_label
            # gtap_r50_labels = pd.read_csv(p.gtapv7_r160_r50_correspondence_path_
            final_cols = [
                'gadm_r263_id',
                'gtapv7_r251_id',
                'gtapv7_r160_id',
                'gtapv7_r50_id',
                'gadm_r263_label',
                'gtapv7_r251_label',
                'gtapv7_r160_label',            
                'gtapv7_r50_label',            
                'gadm_r263_name',
                'gtapv7_r251_name',
                'gtapv7_r160_name',                
                'gtapv7_r50_name',
                'gadm_r263_description',
                'gtapv7_r251_description',
                'gtapv7_r160_description',
                'gtapv7_r50_description',
            ]
            df = df[final_cols]
            
            # Sort to ee spec
            df = df.sort_values('gtapv7_r251_id')
            df = df.sort_values('gtapv7_r160_id')
            df = df.sort_values('gtapv7_r50_id')
            
            # Do this again cause i'm lazy
            df.loc[df['gtapv7_r251_label'] == 'hmd', 'gadm_r263_id'] = 100 
            df.loc[df['gtapv7_r251_label'] == 'hmd', 'gadm_r263_label'] = 'hmd'
            df.loc[df['gtapv7_r251_label'] == 'hmd', 'gadm_r263_name'] = 'Heard Island and McDonald Island'
            df.loc[df['gtapv7_r251_label'] == 'hmd', 'gadm_r263_description'] = 'Heard Island and McDonald Island' 
                        
            
            
            hb.log('df final\n', df, level=100)
            
            # Manually set dtype for two remaining cols. This was not possible to fix via the function
            # df_convert_column_type for unknown reasons, but probably because the nan-value in gadm
            # combined with Operation on a Copy Warning.
            df['gadm_r263_id'] = df['gadm_r263_id'].astype('Int64')
            df['gtapv7_r251_id'] = df['gtapv7_r251_id'].astype('Int64')

            # Save the outer correspondence
            df.to_csv(hb.suri(p.gadm_r263_gtapv7_r251_r160_r50_correspondence_path, ''), index=False)
            
            # Make a final version with a simplified name
            df.to_csv(p.gtapv7_aez_rd_correspondence_path, index=False)
            
    'gtap_aez_seals_correspondence_done is DONE'
    

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
                har_index_path = p.get_path(os.path.join('gtappy', 'data', aggregation_label, hb.file_root(input_har_path) + '.csv'))             
                if hb.path_exists(input_har_path):
                    hb.log('Found input_har_path: ' + input_har_path, level=100)
                    output_dir = p.get_path(os.path.join('gtappy', 'data', aggregation_label))
                    hb.create_directories(output_dir)
                    
                    if not hb.path_exists(har_index_path, verbose=False): # Minor note, could add more robust file validation to check for ALL the implied files to exist.
                    
                        # Extract the har to the indexed DF format.
                        gtappy_file_io.har_to_ndindexed_dfs(input_har_path, har_index_path)                    
                        
                        # # For validation (and actual use in the model), create a new har from the indexed dir.
                        # local_har_path = hb.path_rename_change_dir(input_har_path, output_dir)
                        # gtappy_file_io.indexed_dfs_to_har(har_index_path, local_har_path)



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
            sets_list = gtappy_file_io.get_set_labels_from_index_path(input_indexed_dfs_path)

            # Generatea a "stub" csv which is all the information that should go into the mapping file 
            # that can be inferred from the base data (basically everything besides 
            # the actuall aggregation to a smaller set.)
            mapfile_stub_index_csv_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label, 'mapfile_stub.csv'))
            if not hb.path_exists(mapfile_stub_index_csv_path):
                mapfile_data_dir = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label, 'MapFile'))
                hb.create_directories(mapfile_data_dir)

                # Get rid of unnecessary metadata
                input_indexed_dfs = input_indexed_dfs.loc[input_indexed_dfs['header'].str.startswith('XX')]
                
                # Generate an index_dict, which will be convereted into a dataframe and then saved to Indexed CSV
                columns = input_indexed_dfs.columns
                index_dict = {i : list(input_indexed_dfs[i]) for i in columns} 
                
                # The new mapfile will be saved as an ndCSV (rename of Indexed CSV?)
                index_csv_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label, 'MapFile.csv'))

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
                        data_dir = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label, 'MapFile'))
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
                        data_dir = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label, 'MapFile'))
                        file_path = os.path.join(data_dir, hack_mapping_names[c] + '.csv')
                        data_df.to_csv(file_path, index=False)
                        
                        # sets_dict[hack_mapping_names[c]] = mapping_df[col_name]
                    
                    # Write the header data to CSVs in the indexed folder
                    
                    
                    index_df = pd.DataFrame(data=index_dict)
                    index_df.to_csv(index_csv_path, index=False)
                    # hb.python_object_to_csv(sets_dict, sets_csv_path, csv_type='2d_odict_list')
                            
                    mapfile_har_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label, 'MapFile.har'))
                    if not hb.path_exists(mapfile_har_path):

                        ## START HERE: Almost there. Make it sothat the new Harfiole also has the remappings that Erwin calls in the code, eg.g rin4, rindc, minc, mrin.                
                        gtappy_file_io.indexed_dfs_to_har(index_csv_path, mapfile_har_path)
                          
 
def gtap_runs(p):
    """Run a precompiled gtap exe file by creating a cmf file and calling it.
    
    The current general approach to ingesting a new gtap-aez release from Purdue is:
    
    1. Extract it into the cge_releases dir and update the release name in the run file.
    2. Based on a CMF file you want to use as a template, rename the p1, p2, cmf and other vars to catear-denoted and more descripting labels. Save this as labeled_cmf_template.cmf in the cur_dir.
    3. Using 2, generate a new cmf file for each experiment, year, and aggregation. This will be saved in the cur_dir in the nested folder.
    """


    if p.run_this:
        
        for aggregation_label in p.aggregation_labels:

            run_parallel = 0
            parallel_iterable = []
             
            for experiment_label in p.experiment_labels:
                
                for n_years_counter, ending_year in enumerate(p.years):
                    
                    if n_years_counter == 0:
                        starting_year = p.base_year
                    else:
                        starting_year = p.years[n_years_counter - 1]
                        
                    output_dir = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label, experiment_label, str(ending_year)))
                    expected_sl4_path = os.path.join(output_dir, experiment_label + '_Y' + str(ending_year) + '.sl4')
                    
                    if not hb.path_exists(expected_sl4_path):
                        
                        hb.create_directories(output_dir)
                        
                        generated_cmf_path = os.path.join(output_dir, aggregation_label + '_' + experiment_label + '.cmf')

                        if n_years_counter == 0:
                            current_cge_data_dir = os.path.join(p.cge_data_dir, aggregation_label)
                        else:
                            current_cge_data_dir = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label, experiment_label, str(starting_year)))
                        
                        # CMF: experiment_label # Rename BUT I understand this one might not be changeable because it appears to be defined by the filename of the CMF?
                        # p1: gtap_base_data_dir # EXCLUDE THIS, because we only use it for p2
                        # p2: starting_data_file_path # Rename points to the correct starting har
                        # p3: output_dir # Rename
                        # p4: starting_year # Rename
                        # p5: ending_year # Rename
                        # TODOO Once i get erwin's renamed bat file this step can go away
                        bat_to_catear_vars_replace_dict = {}
                        bat_to_catear_vars_replace_dict['<'] = '<^'
                        bat_to_catear_vars_replace_dict['>'] = '^>' 
                        bat_to_catear_vars_replace_dict['<^CMF^>'] = '<^cmf^>' 
                        bat_to_catear_vars_replace_dict['<^cmf^>'] = '<^experiment_label^>'                    
                        bat_to_catear_vars_replace_dict['<^p1^>'] = '<^gtap_base_data_dir^>' 
                        bat_to_catear_vars_replace_dict['<^p2^>'] = '<^starting_data_file_path^>' 
                        bat_to_catear_vars_replace_dict['<^p3^>'] = '<^output_dir^>' 
                        bat_to_catear_vars_replace_dict['<^p4^>'] = '<^starting_year^>' 
                        bat_to_catear_vars_replace_dict['<^p5^>'] = '<^ending_year^>' 
                        
                        labeled_cmf_template_path = os.path.join(output_dir, 'labeled_cmf_template.cmf')
                        hb.replace_in_file_via_dict(p.template_bau_oldschool_cmf_path, labeled_cmf_template_path, bat_to_catear_vars_replace_dict)
                        
                        scenario_replace_dict = {}
                        scenario_replace_dict['<^experiment_label^>'] = experiment_label
                        scenario_replace_dict['<^gtap_base_data_dir^>'] = os.path.join(p.cge_data_dir, aggregation_label)
                        if n_years_counter == 0:
                            scenario_replace_dict['<^starting_data_file_path^>'] = os.path.join(scenario_replace_dict['<^gtap_base_data_dir^>'], 'basedata.har') 
                        else:
                            scenario_replace_dict['<^starting_data_file_path^>'] = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label, experiment_label, str(starting_year), experiment_label + '_Y' + str(starting_year) + '.upd') )
                        scenario_replace_dict['<^output_dir^>'] = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label, experiment_label, str(ending_year)))
                        scenario_replace_dict['<^starting_year^>'] = 'Y' + str(starting_year)
                        scenario_replace_dict['<^ending_year^>'] = 'Y' + str(ending_year)
                        
                        hb.create_directories(scenario_replace_dict['<^output_dir^>'])

                        scenario_cmf_path = os.path.join(output_dir, aggregation_label + '_' + experiment_label + '.cmf')
                        hb.replace_in_file_via_dict(labeled_cmf_template_path, scenario_cmf_path, scenario_replace_dict)
                        
                        
  
                        
                        # Generate the OS call for the CGE model executable and its corresponding cmf file
                        call_list = [p.cge_executable_path, '-cmf', scenario_cmf_path]                
                        
                        if run_parallel: # When running in paralell, add it to a list for later parallel processing.
                            parallel_iterable.append(tuple([experiment_label, call_list]))

                        else: # Because not running in parallel, just run it right away.                    
                            call_list = [p.cge_executable_path, '-cmf', scenario_cmf_path]
                            gtappy_runner.run_gtap_cmf(scenario_cmf_path, call_list)

                # Now that the iterable is created, run them all in parallel
                num_workers = len(p.experiment_labels)
                if run_parallel:
                    # Performance note: it takes about 3 seconds to run this block even with nothing in the iterable, I guess just from launching the worker pool
                    if len(parallel_iterable) > 0:

                        worker_pool = multiprocessing.Pool(num_workers)  # NOTE, worker pool and results are LOCAL variabes so that they aren't pickled when we pass the project object.

                        finished_results = []
                        result = worker_pool.starmap_async(gtappy_runner.run_gtap_cmf, parallel_iterable)
                        for i in result.get():
                            finished_results.append(i)
                        worker_pool.close()
                        worker_pool.join()


                
        # Note bug. Currently even when done in parallel, only 1 run can be run per python run. Says it runs out of space to allocate results, which isn't true. I'm assuming is an unreleased shared asset, maybe gtap.exe

def indexed_csvs(p):
    
    if p.run_this:
        
        for aggregation_label in p.aggregation_labels:
            
            for experiment_label in p.experiment_labels:
                
                for year in p.years:
                    
                    hb.log('Extracting data via indexed_csvs for', aggregation_label, experiment_label, year, level=100)
                    
                    expected_filenames = [experiment_label + '_Y' + str(year) + '.sl4', 
                                          experiment_label + '_Y' + str(year) + '.UPD', 
                                        #   experiment_label + '-SUM'  + '_Y' + str(year) + '.har',  # CURRENTLY DEACTIVATED BECAUSE OF -1 BUG, waiting for Erwin response
                                          experiment_label + '-VOL'  + '_Y' + str(year) + '.har', 
                                          experiment_label + '-WEL'  + '_Y' + str(year) + '.har',]
                    
                    for filename in expected_filenames:
                        hb.log('Extracting data for ', aggregation_label, experiment_label, filename, level=100)
                        
                        experiment_dir = os.path.join(p.gtap_runs_dir, aggregation_label, experiment_label, str(year))
                        expected_path = os.path.join(experiment_dir, filename)
                        
                        if not hb.path_exists(expected_path, verbose=False):
                            raise NameError('Cannot find file: ' + str(expected_path))
                        
                        output_dir = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label, experiment_label, str(year)))
                        indexed_df_path = os.path.join(output_dir, filename.replace('.', '_') + '.csv')
                        if not hb.path_exists(indexed_df_path):     
                            
                            # START HERE: See if using the sl4 interface makes the sl4 pull in all the actually-used data.
                            if os.path.splitext(filename)[1] == '.sl4':
                                gtappy_file_io.sl4_to_ndindexed_dfs(expected_path, indexed_df_path)
                            else:
                                gtappy_file_io.har_to_ndindexed_dfs(expected_path, indexed_df_path)
                            
                            
                        also_write_validation_hars = False
                        if also_write_validation_hars:
                            # Write a har for validation
                            # har_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label, experiment_label, experiment_label + '.har'))
                            har_path = hb.path_replace_extension(indexed_df_path, '.har')
                            if not hb.path_exists(har_path) and os.path.splitext(filename)[1] != '.sl4'  and os.path.splitext(filename)[1] != '.UPD':
                                gtappy_file_io.indexed_dfs_to_har(indexed_df_path, har_path) 


def stacked_csvs(p):
    

    if p.run_this:
        
        headers_to_stack = p.headers_to_extract
        for aggregation_label in p.aggregation_labels:
            
            for experiment_label in p.experiment_labels:
                
                for year in p.years:
                    filenames_to_extract = [
                        experiment_label + '_Y' + str(year) + '_sl4.csv',
                    ]
                    
                    input_dir = os.path.join(p.indexed_csvs_dir, aggregation_label, experiment_label, str(year))
                    
                    for filename in filenames_to_extract:
                        
                        input_file_path = os.path.join(input_dir, filename)
                        hb.log('Making stacked data  for ', aggregation_label, experiment_label, input_file_path, level=100)
                        output_file_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', hb.file_root(filename) + '_stacked.csv'))
                        headers_to_stack = 'all'
                        # headers_to_stack = ['pds']
                        headers_to_exclude = 'default'
                        
                        # TODOO Note the case sensitivity here
                        
                        
                        if not hb.path_exists(output_file_path):
                            gtappy_file_io.ndstack_indexed_csv(input_file_path, output_file_path, headers_to_stack)
                            
                            
def single_year_tidy_variable_csvs(p):
    # """Eventually will be integrated into the run file, but for now, just hard codes and alternative project 
    # to compare against."""
    
    # # Currently not incorporated into the run file.
    # comparison_dir = "C:/Users/jajohns/Files/gtappy/projects/test_gtappy_aez_project_all_years/intermediate/stacked_csvs"
    
    headers_to_extract = p.headers_to_extract
    if p.run_this:

        for aggregation_label in p.aggregation_labels:
            
            for experiment_label in p.experiment_labels:
                
                for year in p.years:
                        
                    last_output_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label + '_' + experiment_label + '_' + str(year) + '_' + headers_to_extract[-1] + '_simple.csv'))
                    if not hb.path_exists(last_output_path):
                        src_csv_path = os.path.join(p.stacked_csvs_dir, experiment_label + '_Y' + str(year) + '_sl4_stacked.csv')
                        
                        # Use the custom read_ndstacked_csv function to read the stacked csv
                        src_df = gtappy_file_io.read_ndstacked_csv(src_csv_path)
                        # src_df = pd.read_csv(src_csv_path)                        
                        
                        for header in [i.lower() for i in headers_to_extract]:                                
                                
                            output_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label + '_' + experiment_label + '_' + str(year) + '_' + header + '_simple.csv'))
                            
                            if not hb.path_exists(output_path):
                                hb.log('Making single_year_tidy_variable_csvs for ', aggregation_label, experiment_label, year, header, level=100)
                                # select just where the header column equals header
                                src_df_header = src_df.loc[src_df['header'] == header]
                                
                                src_simplified_df = gtappy_utils.ndstacked_df_to_tidy_df(src_df_header)
                                
                                src_simplified_df.to_csv(output_path, index=False)
                                
                                hb.log('Summing all values in ', header, 'from', output_path, level=9)
                                hb.log(src_simplified_df['value'].sum(), level=9)
                            
                            # SWITCHED HERE to converting run_test_cwon to be up to date and be the "comparative static" run mode.
 
    pass                                   

def combined_stacked_results_across_years(p):
    """ Use teh stacked dfs directly. Is more brute-force but doesn't require having generated the correct simplified CSVs before."""
    if p.run_this:
        headers = p.headers_to_extract
        headers_to_extract_time_series = {}
        for header in p.headers_to_extract:
            headers_to_extract_time_series[header] = []
        
        for aggregation_label in p.aggregation_labels:
            
            for experiment_label in p.experiment_labels:
                
                # check if last exists:
                check_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label + '_' + experiment_label + '_' + headers[-1] + '_stacked_time_series.csv'))
                if not hb.path_exists(check_path):
                
                    for year in p.years:
                        
                        current_stacked_csv_path = os.path.join(p.stacked_csvs_dir, experiment_label + '_Y' + str(year) + '_sl4_stacked.csv')
                        # 
                        
                        

                        df = pd.read_csv(current_stacked_csv_path)
                        hb.log(header, aggregation_label, experiment_label, year, df)

                        for header in headers:


                            if header.lower() == 'p_landcover_l':
                                pass

                            subset = df.loc[df['header'].str.lower() == header.lower()]
                            subset['year'] = year
                            # subset.loc['year'] = year
                            headers_to_extract_time_series[header].append(subset)
                        
                        
                    # concatenate each header into a single df
                    for header in headers:
                        output_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label + '_' + experiment_label + '_' + header + '_stacked_time_series.csv'))
                        df = pd.concat(headers_to_extract_time_series[header])
                        df.to_csv(output_path, index=False)
                    
                        
def single_variable_time_series(p):
    """ Requires single_year_tidy_variable_csvs to be run for the same headers"""
    if p.run_this:
        headers = p.headers_to_extract
        headers_to_extract_time_series = {}
        for header in headers:
            headers_to_extract_time_series[header] = []
        
        for aggregation_label in p.aggregation_labels:
            
            for experiment_label in p.experiment_labels:
                last_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label + '_' + experiment_label + '_' + headers[-1] + '_time_series.csv'))
                if not hb.path_exists(last_path):
                
                
                    for year in p.years:
                        

                        for header in headers:
                            current_tidy_csv_path = os.path.join(p.single_year_tidy_variable_csvs_dir, aggregation_label + '_' + experiment_label + '_' + str(year) + '_' + header + '_simple.csv')
                            df = pd.read_csv(current_tidy_csv_path)
                            hb.log(df)                        
                            
                            subset = df.loc[df['header'].str.lower() == header.lower()]
                            subset['year'] = year
                            headers_to_extract_time_series[header].append(subset)
                            
                            
                    # concatenate each header into a single df
                    for header in headers:
                        df = pd.concat(headers_to_extract_time_series[header])
                        df.to_csv(os.path.join('gtappy', 'aggregation_mappings', aggregation_label + '_' + experiment_label + '_' + header + '_time_series.csv'), index=False)
                        
                            
                        
                                     

def econ_vizualization(p):
    pass

def econ_time_series(p):
    if p.run_this:

        # vars_to_plot = ['qgdp', 'p_ECONLAND']
        vars_to_plot = p.headers_to_extract
   
        for aggregation_label in p.aggregation_labels:            
            for experiment_label in p.experiment_labels:
                for c, var in enumerate(vars_to_plot):
                    input_csv_path = os.path.join(p.single_variable_time_series_dir, aggregation_label + '_' + experiment_label + '_' + var + '_time_series.csv')
                    output_png_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label, experiment_label, aggregation_label + '_' + experiment_label + '_' + var + '_time_series.png') )
                    if not hb.path_exists(output_png_path):
                        hb.create_directories(output_png_path)
                        
                        df = pd.read_csv(input_csv_path)
                        hb.log(df, level=100)
                       
                        annual_aggregated_values = df.groupby('year')['value'].sum()
                        hb.log(annual_aggregated_values, level=100)
                        
                        # Create a figure and plot the aggregated values
                        fig, ax = plt.subplots()
                        ax = annual_aggregated_values.plot(kind='line', rot=0, colormap='viridis', figsize=(10, 6))
                        
                        # Set labels and title
                        ax.set_xlabel('Year')
                        ax.set_ylabel('Percent change in ' + var)
                        ax.set_title('Global percent change in ' + var + ' by year')
                        # if legend_labels is not None:
                        #     plt.legend(title='Shock', loc='upper left', labels=legend_labels)

                        # plt.axhline(y=0, color='gray', linestyle='dotted', linewidth=1, label='100%')

                        # Save the png to output_png_path
                        fig.savefig(output_png_path, dpi=300, bbox_inches='tight')               
                                
    5
    
def econ_lcovercom(p):
    if p.run_this:

        vars_to_plot = ['lcovercom']
        # vars_to_plot = p.headers_to_extract
   
        for aggregation_label in p.aggregation_labels:            
            for experiment_label in p.experiment_labels:
                for c, var in enumerate(vars_to_plot):
                    input_csv_path = os.path.join(p.single_variable_time_series_dir, aggregation_label + '_' + experiment_label + '_' + var + '_time_series.csv')
                    

                    
                    
                    df = pd.read_csv(input_csv_path)
                    
                    hb.log('Plotting econ_lcovercom', level=100)
                    hb.log('\n\nInput\n', df, level=100)
                    # Pivot the table
                    pivot_table = df.pivot_table(index='REG', columns=['year', 'PRODLCOV'], values='value')

                    hb.log('\n\nPivoted\n', pivot_table, level=100)
                    
                    countries_to_plot = ['bgd', 'chn', 'gbr', 'bra']
                    
                    for country in countries_to_plot:
                        output_png_path = p.get_path(os.path.join('gtappy', 'aggregation_mappings', aggregation_label, experiment_label, aggregation_label + '_' + experiment_label + '_' + var + '_' + country + '_time_series.png') )
                        hb.create_directories(output_png_path)
                        hb.log('Plotting econ_lcovercom for ' + country, level=100)
                        # Create a figure and plot the aggregated values
                        fig, ax = plt.subplots()
                        country_df = pivot_table.loc[country.rstrip()].unstack()
                        country_df['Natural'] = 0 - country_df['Cropland'] - country_df['Pastureland'] - country_df['Forestland']
                        hb.log('\n\nCountry\n', country_df, level=100)
                        
                        # make a list of 15 possible colors in matplotlib using just the text
                        color_list = []
                        for i in range(15):
                            color_list.append(plt.cm.tab20(i))

                        for c_ltype, ltype in enumerate(country_df.columns):
                            ax = country_df[ltype].plot(kind='line', rot=0,  figsize=(10, 6), label=ltype, color=color_list[c_ltype])
                            
                            # Set labels and title
                            ax.set_xlabel('Year')
                            ax.set_ylabel('Million hectares')
                            ax.set_title('Global hectarage change in ' + var + ' by year')
                            # if legend_labels is not None:
                            #     plt.legend(title='Shock', loc='upper left', labels=legend_labels)

                            # plt.axhline(y=0, color='gray', linestyle='dotted', linewidth=1, label='100%')

                            # Save the png to output_png_path
                            
                        # Add a zero line
                        plt.axhline(y=0, color='gray', linestyle='dotted', linewidth=1, label='100%')
                        
                        # Add a legend
                        plt.legend(title='Land cover type', loc='upper right', labels=country_df.columns)
                        fig.savefig(output_png_path, dpi=300, bbox_inches='tight')
                        # # Create a figure and plot the aggregated values
                        # fig, ax = plt.subplots()
                        # ax = pivot_table.plot(kind='line', rot=0, colormap='viridis', figsize=(10, 6))
                        
                        # # Set labels and title
                        # ax.set_xlabel('Year')
                        # ax.set_ylabel('Percent change in ' + var)
                        # ax.set_title('Global percent change in ' + var + ' by year')
                        # # if legend_labels is not None:
                        # #     plt.legend(title='Shock', loc='upper left', labels=legend_labels)

                        # # plt.axhline(y=0, color='gray', linestyle='dotted', linewidth=1, label='100%')

                        # # Save the png to output_png_path
                        # fig.savefig(output_png_path, dpi=300, bbox_inches='tight')               
                                
    5