import os
import hazelbean as hb

def generate_cmf_dict_from_cmf_file(template_cmf_path, replace_dict):
    def process_string(input_string, replace_dict):
        initial_replace_dict = {
            '<': '<^',
            '>': '^>',
            '<^CMF^>': '<^cmf^>',
        }
        
        initial_replace_dict.update(replace_dict)
        replace_dict = initial_replace_dict

        for k, v in replace_dict.items():
            input_string = input_string.replace(k, str(v))
        
        output_string = input_string
        return output_string.lstrip().rstrip()
    
    # Open the cmf and iterate through its lines
    with open(template_cmf_path, 'r') as rf:
        cmf_lines = rf.readlines()
        output_dict = {}
        # in_exogenous = False
        for c, line in enumerate(cmf_lines):
            
            # if 'endogenous' in line.lower():
            #     print(line)
            
            
            # line = line.replace('\n', '')
            # spaces_removed = line.replace(' ', '')
            # Check for empty but for strings line
            # if len(spaces_removed) == 0:
                # only_spaces = True
            # else:
                # only_spaces = False
            
            # if not line.startswith('!') and not only_spaces and not line.lower().lstrip().startswith('shock'):
            # output_dict[c] = process_string(spaces_removed, replace_dict) 
            
            if 0:    
                # Strip anything on ta line after a comment starts.
                line_comment_split = line.split('!')
                if len(line_comment_split) > 1:
                    line = line_comment_split[0]
                if line.lstrip().startswith('Exogenous'):
                    if ';' in line:
                        # Then it is a single-line Exogenous statement
                        if '=' in line:
                            split_equal_line = line.split('=')
                            output_dict[process_string(split_equal_line[0], replace_dict)] = process_string(split_equal_line[1], replace_dict) 

                        else:
                            # Handle the case where it's a single line but just variable labels
                            line_space_split = line.split(' ')
                            output_dict['Exogenous'] = [process_string(i, replace_dict) for i in line_space_split[1:]]        
                        if in_exogenous:
                            in_exogenous = False
       
                    else:
                        in_exogenous = True
                        exogenous_list = []
                        
                
                elif in_exogenous:
                    split_line = [i for i in line.split(' ') if i != '' and 'Exogenous' not in i]

                    if ';' not in line:
                        exogenous_list.extend(split_line)
                    else:
                        in_exogenous = False
                        output_dict['Exogenous'] = exogenous_list


                elif '=' in line:
                    split_equal_line = line.split('=')
                    output_dict[process_string(split_equal_line[0], replace_dict)] = process_string(split_equal_line[1], replace_dict) 
                else:
                    if 'Rest endogenous;' in line:
                        output_dict['Rest endogenous;'] = ''
                    else:
                        raise ValueError('Line does not have an equals sign and is not a Rest endogenous line: ' + str(line))    
                
    # hb.log('Generated CMF dict from file: ' + str(template_cmf_path))
    # hb.log(hb.print_dict(output_dict))
    
    return output_dict


def generate_cmf_dict_from_cmf_file_old(template_cmf_path, replace_dict):
    def process_string(input_string, replace_dict):
        initial_replace_dict = {
            '<': '<^',
            '>': '^>',
            '<^CMF^>': '<^cmf^>',
        }
        
        initial_replace_dict.update(replace_dict)
        replace_dict = initial_replace_dict
        # %MODd%\%MODEL% -cmf %SIMRUN%.cmf -p1=%DATd% -p2=%DATd%\basedata.har          -p3=%SOLd% -p4=%YEAR00% -p5=%YEAR01%
        
        """
set PROJ not used... implied by projectflow
set AGG=gtapaez11-50 # Was only used to set DATd, so set that directly
set MODEL=gtapv7-aez-rd # ONly used with MODd, so set directly
set SIMRUN=%PROJ%_bau # Switch this to just counterfactual name
set YEAR00=Y2017
set MODd=..\mod
set SOLd=..\out
set CMFd=.\cmf
set DATd=..\data\%AGG%
"""
        for k, v in replace_dict.items():
            input_string = input_string.replace(k, str(v))
        
        output_string = input_string
        return output_string.lstrip().rstrip()
    
    # Open the cmf and iterate through its lines
    with open(template_cmf_path, 'r') as rf:
        cmf_lines = rf.readlines()
        output_dict = {}
        in_exogenous = False
        for line in cmf_lines:
            
            if 'endogenous' in line.lower():
                print(line)
            
            
            line = line.replace('\n', '')
            spaces_removed = line.replace(' ', '')
            # Check for empty but for strings line
            if len(spaces_removed) == 0:
                only_spaces = True
            else:
                only_spaces = False
            
            if not line.startswith('!') and not only_spaces and not line.lower().lstrip().startswith('shock'):
                
                # Strip anything on ta line after a comment starts.
                line_comment_split = line.split('!')
                if len(line_comment_split) > 1:
                    line = line_comment_split[0]
                if line.lstrip().startswith('Exogenous'):
                    if ';' in line:
                        # Then it is a single-line Exogenous statement
                        if '=' in line:
                            split_equal_line = line.split('=')
                            output_dict[process_string(split_equal_line[0], replace_dict)] = process_string(split_equal_line[1], replace_dict) 

                        else:
                            # Handle the case where it's a single line but just variable labels
                            line_space_split = line.split(' ')
                            output_dict['Exogenous'] = [process_string(i, replace_dict) for i in line_space_split[1:]]        
                        if in_exogenous:
                            in_exogenous = False
       
                    else:
                        in_exogenous = True
                        exogenous_list = []
                        
                
                elif in_exogenous:
                    split_line = [i for i in line.split(' ') if i != '' and 'Exogenous' not in i]

                    if ';' not in line:
                        exogenous_list.extend(split_line)
                    else:
                        in_exogenous = False
                        output_dict['Exogenous'] = exogenous_list


                elif '=' in line:
                    split_equal_line = line.split('=')
                    output_dict[process_string(split_equal_line[0], replace_dict)] = process_string(split_equal_line[1], replace_dict) 
                else:
                    if 'Rest endogenous;' in line:
                        output_dict['Rest endogenous;'] = ''
                    else:
                        raise ValueError('Line does not have an equals sign and is not a Rest endogenous line: ' + str(line))    
                
    hb.log('Generated CMF dict from file: ' + str(template_cmf_path))
    hb.log(hb.print_dict(output_dict))
    
    return output_dict
    

def generate_cmf_file_for_scenario(inputs_dict, 
                                    experiment_name,                                                    
                                    data_dir,
                                    output_dir,     
                                    generated_cmf_path,                                                
                                    # model_dir='..\\mod', 
                                    # solution_output_dir='..\\out', 
                                    # cmfs_dir='.\\cmf', 
                                    # data_dir='..\\data', 
                                    # aggregation_name='65x141'
                                    ):
    
    reserved_keys = ['xSets', 'xSubsets', 'Exogenous', 'Shocks', 'cmf_commands']
    output_list = []
    for k, v in inputs_dict.items():
        if 'endo' in k.lower():
            print(k)
        if k not in reserved_keys:
            if '<^experiment_name^>' in v:   
                v = v.replace('<^experiment_name^>', experiment_name)  
            if '<^data_dir^>' in v:
                v = v.replace('<^data_dir^>', data_dir)           
            if '<^output_dir^>' in v:
                v = v.replace('<^output_dir^>', output_dir)         
            if len(v) > 0:
                output_list.append(str(k) + '=' + str(v) + '\n')
            else:
                output_list.append(str(k) + '\n')
                
        elif k == 'xSets':
            for xset_name, xset_values_list in v.items():                    
                output_list.append('xSet ' + str(xset_name) + ' # ' + str(xset_values_list[0]) + ' # ' + str(xset_values_list[1]) + ';\n')
        elif k == 'xSubsets':
            for xSubset_name in v:                    
                output_list.append('xSubset ' + str(xSubset_name) + ';\n')
        elif k == 'Exogenous':
            if isinstance(v, list):
                output_list.append('Exogenous\n')
                for i in v:
                    output_list.append('          ' + str(i) + '\n')
                output_list.append('          ;\n')
                # output_list.append('Rest endogenous;\n')
        elif k == 'Shocks':
            output_list.append(v['shock_string'])
                   
    with open(generated_cmf_path, 'w') as wf:
        for line in output_list:
            wf.writelines(line)

    
    # hb.write_to_file(output_list, generated_cmf_path)                   
        
    
    # -p1=%DATd% -p2=%SOLd%
    5


gtap_v7_cmf_dict = {
            'auxiliary files': 'gtapv7',
            'check-on-read elements': 'warn',
            'cpu': 'yes',
            'start with MMNZ': '2147483646',
            'MA48 increase_MMNZ': 'veryfast',
            'File GTAPSETS': '\"<^data_dir^>\SETS.har\"', 
            'File GTAPDATA': '\"<^data_dir^>\Basedata.har\"',
            'File GTAPPARM': '\"<^data_dir^>\Default.prm\"',
            'File GTAPSUPP': '\"<^data_dir^>\MapFile.har\"',
            'Updated File GTAPDATA': '\"<^output_dir^>\<^experiment_name^>.UPD\"',
            'File GTAPVOL': '\"<^output_dir^>\<^experiment_name^>-VOL.har\"',
            'File WELVIEW': '\"<^output_dir^>\<^experiment_name^>-WEL.har\"',
            'File GTAPSUM': '\"<^output_dir^>\<^experiment_name^>-SUM.har\"',
            'Solution File': '\"<^output_dir^>\<^experiment_name^>.sl4\"',
            'log file': '\"<^output_dir^>\<^experiment_name^>.log\"',
            'Method': 'Gragg',
            'Steps': '2 4 6',
            'Exogenous': [
                'pop',
                'psaveslack' ,
                'pfactwld',
                'profitslack',
                'incomeslack ',
                'endwslack',
                'cgdslack',
                'tradslack',
                'ams' ,
                'atm',
                'atf',
                'ats',
                'atd',
                'aosec',
                'aoreg',
                'avasec',
                'avareg',
                'aintsec',
                'aintreg',
                'aintall',
                'afcom',
                'afsec',
                'afreg',
                'afecom',
                'afesec',
                'afereg',
                'aoall',
                'afall',
                'afeall',
                'au',
                'dppriv',
                'dpgov',
                'dpsave',
                'to',
                'tinc',
                'tpreg',
                'tm',
                'tms',
                'tx',
                'txs',
                'qe',
                'qesf',
            ], 
            'Verbal Description': 'verbal_description_default_text',

    }


gtap_v7b_cmf_dict = {
            'auxiliary files': 'gtapv7',
            'check-on-read elements': 'warn',
            'cpu': 'yes',
            'start with MMNZ': '2147483646',
            'MA48 increase_MMNZ': 'veryfast',
            'File GTAPSETS': '\"<^data_dir^>\SETS.har\"', 
            'File GTAPDATA': '\"<^data_dir^>\Basedata.har\"',
            'File GTAPPARM': '\"<^data_dir^>\Default.prm\"',
            # 'File GTAPSUPP': '\"<^data_dir^>\Basedata.har\"',
            'File GTAPSUPP': '\"<^data_dir^>\MapFile.har\"',
            'Updated File GTAPDATA': '\"<^output_dir^>\<^experiment_name^>.UPD\"',
            'File GTAPVOL': '\"<^output_dir^>\<^experiment_name^>-VOL.har\"',
            'File WELVIEW': '\"<^output_dir^>\<^experiment_name^>-WEL.har\"',
            'File GTAPSUM': '\"<^output_dir^>\<^experiment_name^>-SUM.har\"',
            'Solution File': '\"<^output_dir^>\<^experiment_name^>.sl4\"',
            'log file': '\"<^output_dir^>\<^experiment_name^>.log\"',
            'Method': 'Gragg',
            'Steps': '2 4 6',
            'Exogenous': [
                'pop',
                'psaveslack' ,
                'pfactwld',
                'profitslack',
                'incomeslack ',
                'endwslack',
                'cgdslack',
                'tradslack',
                'ams' ,
                'atm',
                'atf',
                'ats',
                'atd',
                'aosec',
                'aoreg',
                'avasec',
                'avareg',
                'aintsec',
                'aintreg',
                'aintall',
                'afcom',
                'afsec',
                'afreg',
                'afecom',
                'afesec',
                'afereg',
                'aoall',
                'afall',
                'afeall',
                'au',
                'dppriv',
                'dpgov',
                'dpsave',
                'to',
                'tinc',
                'tpreg',
                'tm',
                'tms',
                'tx',
                'txs',
                'qe',
                'qesf',
            ], 
            'Verbal Description': 'verbal_description_default_text',

    }


prefinished_gtap_v7_cmf_dict = {
            'auxiliary files': 'gtapv7',
            'check-on-read elements': 'warn',
            'cpu': 'yes',
            'start with MMNZ': '2147483646',
            'File GTAPSETS': '\"<^data_dir^>\SETS.har\"', 
            'File GTAPDATA': '\"<^data_dir^>\Basedata.har\"',
            'File GTAPPARM': '\"<^data_dir^>\Default.prm\"',
            # 'File GTAPSUPP': '\"<^data_dir^>\Basedata.har\"',
            'File GTAPSUPP': '\"<^data_dir^>\MapFile.har\"',
            'Updated File GTAPDATA': '\"<^output_dir^>\<^experiment_name^>.UPD\"',
            'File GTAPVOL': '\"<^output_dir^>\<^experiment_name^>-VOL.har\"',
            'File WELVIEW': '\"<^output_dir^>\<^experiment_name^>-WEL.har\"',
            'File GTAPSUM': '\"<^output_dir^>\<^experiment_name^>-SUM.har\"',
            'Solution File': '\"<^output_dir^>\<^experiment_name^>.sl4\"',
            'log file': '\"<^output_dir^>\<^experiment_name^>.log\"',
            'Method': 'Gragg',
            'Steps': '2 4 6',
            'Exogenous': [
                'pop',
                'psaveslack' ,
                'pfactwld',
                'profitslack',
                'incomeslack ',
                'endwslack',
                'cgdslack',
                'tradslack',
                'ams' ,
                'atm',
                'atf',
                'ats',
                'atd',
                'aosec',
                'aoreg',
                'avasec',
                'avareg',
                'aintsec',
                'aintreg',
                'aintall',
                'afcom',
                'afsec',
                'afreg',
                'afecom',
                'afesec',
                'afereg',
                'aoall',
                'afall',
                'afeall',
                'au',
                'dppriv',
                'dpgov',
                'dpsave',
                'to',
                'tinc',
                'tpreg',
                'tm',
                'tms',
                'tx',
                'txs',
                'qe',
                'qesf',
            ], 
            'Verbal Description': 'verbal_description_default_text',
            'xSets': {'AGCOM': ['Agri commodities', '(pdr, wht, gro, v_f, osd, c_b, pfb, ocr, ctl, oap, rmk, wol)'],
                      'AGCOM_SM' : ['smaller agri commodities', '(pdr, wht, gro)'],       
                      },
            # SUGGESTED CHANGE: Use xSet [name of set] read elements from file [file path] header "FOUR LETTER HEADER";
            'xSubsets': ['AGCOM is subset of COMM', 'AGCOM is subset of ACTS', 'AGCOM_SM is subset of COMM', 'AGCOM_SM is subset of ACTS'],
            # xSubset now will be defined based on the same xset file.
            
            
            'Shock': {'name': 'agri_productivity increases 20p',
                      'shortname': 'agpr20',
                      'shock_string': 'Shock aoall(AGCOM_SM, reg) = uniform 20;'}
                
    }

gtap_aez_cmf_dict = {
            'auxiliary files': 'gtapv7',
            'check-on-read elements': 'warn',
            'cpu': 'yes',
            'nds': 'yes',
            'start with MMNZ': '2147483646',
            'MA48 increase_MMNZ': 'veryfast',
            'Extrapolation accuracy file': 'NO',
            'File GTAPSETS': '\"<^data_dir^>\SETS.har\"', 
            'File GTAPDATA': '\"<^data_dir^>\Basedata.har\"',
            'File GTAPPARM': '\"<^data_dir^>\Default.prm\"',
            'file GTAPSUPP': '\"<^data_dir^>\<^experiment_name^>_SUPP.har\"',
            'Updated File GTAPDATA': '\"<^output_dir^>\<^experiment_name^>.UPD\"',
            'file GTAPSUM': '\"<^data_dir^>\<^experiment_name^>_sum.har\"',
            'Solution File': '\"<^output_dir^>\<^experiment_name^>.sl4\"',
            'Verbal Description': '<^experiment_name^>',
            'log file': '\"<^output_dir^>\<^experiment_name^>.sl4\"',
            'Method': 'Euler',
            'Steps': '2 4 6',
            'automatic accuracy': 'yes',
            'accuracy figures': '4',
            'accuracy percent': '90',
            'minimum subinterval length': '0.0001',
            'minimum subinterval fails': 'stop',
            'accuracy criterion': 'Both',
            'subintervals': '5',
            'Exogenous': [
                'pop',
                'psaveslack pfactwld',
                'profitslack incomeslack endwslack',
                'cgdslack tradslack',
                'ams atm atf ats atd',
                'aosec aoreg ',
                'avasec avareg',
                'afcom afsec afreg afecom afesec afereg',
                'aoall afall afeall aoall2 aoall3 aoall4',
                'au dppriv dpgov dpsave',
                'to_1 to_2 !to',
                '!EC change for revenue neutral scenario',
                'tfijr',
                'tfreg',
                '!End: EC change for revenue neutral scenario',
                'tp tm tms tx txs',
                'qo("UnSkLab",REG) ',
                'qo("SkLab",REG) ',
                'qo("Capital",REG) ',
                'qo("NatRes",REG)',
                'tfm tfd  ',

            ],  # STATUS: Gave up on this till i get erwin's newest version. Switching to gtap_v7_cmf_dict for now.
            # 'Exogenous': [
            #     'p_slacklandr',
            #     'p_slacklandr',
            #     'p_slacklandr',
            #     'p_slacklandr',
            #     'p_slacklandr',
            #     'p_slacklandr',
            #     'p_slacklandr',
            # ],

            # START HERE: Get the syntax right for these more complex cases.
            # Make the file-based substitution of specific parameters work easily.

            # p_slacklandr;
            # 'Exogenous': p_ECONLAND  = zero value on    file <p1>\basedata.har header "MAXL" ;
            # 'Exogenous': p_slackland = nonzero value on file <p1>\basedata.har header "MAXL" ; 
            # 'Exogenous': p_LANDCOVER_L(AEZ_COMM,"UNMNGLAND",REG);
            # 'Exogenous': c_MAX_LAND;            
            # 'nds': 'yes',
            # 'nds': 'yes',
            # 'nds': 'yes',


}

"""


Exogenous p_slacklandr;
Exogenous p_ECONLAND  = zero value on    file <p1>\basedata.har header "MAXL" ;
Exogenous p_slackland = nonzero value on file <p1>\basedata.har header "MAXL" ; 
Exogenous p_LANDCOVER_L(AEZ_COMM,"UNMNGLAND",REG);
Exogenous c_MAX_LAND;

Rest Endogenous ;
!===========  
! xSets
!===========  
xSet    CROPS (paddyrice, wheat, crsgrns, fruitveg, oilsds, sugarcrps, cotton, othercrps);
xSubset CROPS is subset of TRAD_COMM;

!===========  
! Swap
!===========  
! Endogenize productivity to target real GDP
swap aoreg = qgdpfisher;

!===========  
! Shocks
!===========  
! (1) GDP, labor and population shocks
Shock qgdpfisher              = file <p3>\ALLSHOCKS.har header "M142" slice "qgdp";
Shock qo("UnSkLab",REG)       = file <p3>\ALLSHOCKS.har header "M142" slice "usklab";
Shock qo("SkLab",REG)         = file <p3>\ALLSHOCKS.har header "M142" slice "sklab";
Shock qo("Capital",REG)       = file <p3>\ALLSHOCKS.har header "M142" slice "cap";
Shock pop                     = file <p3>\ALLSHOCKS.har header "M142" slice "pop";

! (2) Productivity shocks
Shock aoall("paddyrice",REG)  = file <p3>\ALLSHOCKS.har header "S142" slice "crops";
Shock aoall("wheat",REG)      = file <p3>\ALLSHOCKS.har header "S142" slice "crops";
Shock aoall("crsgrns",REG)    = file <p3>\ALLSHOCKS.har header "S142" slice "crops";
Shock aoall("fruitveg",REG)   = file <p3>\ALLSHOCKS.har header "S142" slice "crops";
Shock aoall("oilsds",REG)     = file <p3>\ALLSHOCKS.har header "S142" slice "crops";
Shock aoall("sugarcrps",REG)  = file <p3>\ALLSHOCKS.har header "S142" slice "crops";
Shock aoall("cotton",REG)     = file <p3>\ALLSHOCKS.har header "S142" slice "crops";
Shock aoall("othercrps",REG)  = file <p3>\ALLSHOCKS.har header "S142" slice "crops";

Shock aoall("ruminant",REG)   = file <p3>\ALLSHOCKS.har header "S142" slice "rum";
Shock aoall("nonruminant",REG)= file <p3>\ALLSHOCKS.har header "S142" slice "nrum";

! (3) Productivity gap between services and manufactures
Shock aoall("Mnfcing",REG)    = uniform 0.870;

! (4) Productivity for forestry based on crop sector productivity
Shock afeall(AEZ_COMM,"forestsec",REG) = uniform 10.062;

!===========  
! Subtotal
!===========
Subtotal qgdpfisher qo("UnSkLab",REG) qo("SkLab",REG) qo("Capital",REG) pop = growth;
Subtotal aoall  = TFP;
Subtotal afeall = LandTFP;
"""