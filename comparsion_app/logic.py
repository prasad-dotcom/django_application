# %%
#****************************************************************************************************************************
#                                                                                                                           *
# INSTRUCTION: FOR VENDOR PARTNER USING THIS TDD REVIEW PROGRAM...                                                          *
# PLEASE DOWNLOAD THE LATEST CONFIGURATION FILE FROM THE LINK BELOW AND STORE IN LOCAL FOLDER BEFORE YOU RUN THIS PROGRAM   *
# Ctrl+ https://teams.mdlz.com/:x:/s/dadocumentrepository/EXa_rN8xWx5IrabH9JLa8JoBeVKwza8egKYlBqVcJi73rA?e=EfgEI2           *
#                                                                                                                           *
#****************************************************************************************************************************

# %%
# import all required libraries & packages

import pandas as pd
import tkinter as tk, tkinter.filedialog as tkfd
import sys, os
from IPython.display import display

pd.set_option('display.max_colwidth', None)
pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', None)

# %%
# set global parameters

#loc = "C:/Users/VXJ1871/OneDrive - MDLZ/mdlz-gcp"
#loc = "C:/Users/lhc6760/OneDrive - MDLZ/Sunil Pitta-Main/MDLZ/Projects/TDD Review-All"
validchar_list1 = ["a","b","c","d","e","f","g","h","i","j","k","l","m"]
validchar_list2 = ["n","o","p","q","r","s","t","u","v","w","x","y","z"]
validchar_list3 = ["0","1","2","3","4","5","6","7","8","9","_"]
validchar_list4 = ["."] # v7.0
validchar_list5 = ["A","B","C","D","E","F","G","H","I","J","K","L","M"] # v7.0
validchar_list6 = ["N","O","P","Q","R","S","T","U","V","W","X","Y","Z"] # v7.0
validchar_list = validchar_list1 + validchar_list2 + validchar_list3
validchar_list_iot = validchar_list1 + validchar_list2 + validchar_list3 + validchar_list4  + validchar_list5 + validchar_list6 # v7.0
lcase_charlist = validchar_list1 + validchar_list2
ignore_list = ["n/a", "n.a", "na", "tbd", "tbc"]
dtyp_list = ["curr", "quan", "dec", "fltp", "int", "int64", "integer", "decimal", "int2", "int64", "int32"]
iot_obj_type_list = ["stream", "realtime", "nearrealtime", "archival"] # v7.0


# %%
# prepare common functions

df_log = pd.DataFrame(columns = ['msg-num', 'msg-type', 'xl-worksheet', 'xl-row', 'msg-text'])

def log_message(iTyp, iWksht, iRow, iMsg): # function to log a message
    global df_log
    if iTyp == 'e':
        iTyp, iNum = 'error', 0
    elif iTyp == 'w':
        iTyp, iNum = 'warning', 1
    elif iTyp == 'i':
        iTyp, iNum = 'info', 2
    df_log = pd.concat([df_log, pd.DataFrame([{'msg-num':iNum, 'msg-type':iTyp, 'xl-worksheet':iWksht, 'xl-row':iRow, 'msg-text':iMsg}])], axis=0, ignore_index=True)

def log_sort_index(): # function to sort logged messages before display
    global df_log
    df_log.sort_values(by = ['msg-num', 'xl-worksheet', 'xl-row', 'msg-text'], inplace=True)
    df_log.reset_index(drop=True, inplace=True)
    log_disp_list = ['msg-type', 'xl-worksheet', 'xl-row', 'msg-text']
    df_log = df_log[log_disp_list]

def build_error_string(i_obj_type, i_curr_row): # function to build custom error string per object type
    if i_obj_type == "src":
        e1, e2, e3 = "source", i_curr_row['source_file'], 'source_field_name'
    elif i_obj_type == "gcs":
        e1, e2, e3 = "gcs", i_curr_row['gcs_folder'] + i_curr_row['gcs_file'], 'gcs_field_name'
    elif i_obj_type == "rt":
        e1, e2, e3 = "raw table", i_curr_row['raw_dataset'] + "." + i_curr_row['raw_table'], 'raw_table_field_name'
    elif i_obj_type == "rv":
        e1, e2, e3 = "raw view", i_curr_row['raw_dataset'] + "." + i_curr_row['raw_view'], 'raw_view_field_name'
    elif i_obj_type == "ht":
        e1, e2, e3 = "harmonized table", i_curr_row['harmonized_dataset'] + "." + i_curr_row['harmonized_table'], 'harmonized_table_field_name'
    elif i_obj_type == "hv":
        e1, e2, e3 = "harmonized view", i_curr_row['harmonized_dataset'] + "." + i_curr_row['harmonized_view'], 'harmonized_view_field_name'
    return e1, e2, e3

def key_check(i_obj_type, i_curr_row): # function to check and report for table key availability in a file / table / view
    tCount = xCount = eCount = oCount = 0
    
    tCount = len(set(i_curr_row['key']))
    xCount = [x.strip().lower() for x in set(i_curr_row['key'])].count('x')
    eCount = [x.strip() for x in set(i_curr_row['key'])].count('')
    oCount = tCount - xCount - eCount

    e1, e2, e3 = build_error_string(i_obj_type, i_curr_row)

    if tCount < 1:
        e =  e2 + ' has no keys defined'
        log_message('e', e1, '-', str(e))
    else:
        if xCount < 1:
            e = e2 + ' has no keys marked X'
            log_message('e', e1, '-', str(e))
        if oCount > 0:
            e = e2 + ' has key values other than X or blank'
            log_message('e', e1, '-', str(e))

def dim_fact_check(i_obj_type, i_curr_row): # function to check and report dim / fact availability in a file / table / view
    tCount = dCount = fCount = oCount = 0
    
    tCount = len(i_curr_row['dim_/_fact'])
    dCount = [x.lower().strip() for x in i_curr_row['dim_/_fact']].count('dim')
    fCount = [x.lower().strip() for x in i_curr_row['dim_/_fact']].count('fact')
    oCount = tCount - dCount - fCount

    e1, e2, e3 = build_error_string(i_obj_type, i_curr_row)

    if tCount < 1:
        e = e1 + ' has neither dim(s) nor fact(s)'
        log_message('e', e2, '-', str(e))
    else:
        if dCount < 1:
            e = e2 + ' has no dim(s)'
            log_message('e', e1, '-', str(e))
        if fCount < 1:
            e = e2 + ' has no fact(s)'
            log_message('w', e1, '-', str(e))
        if oCount > 0:
            e = e2 + ' has value(s) other than Dim / Fact'
            log_message('e', e1, '-', str(e))

def duplicate_field_check(i_obj_type, i_curr_row): # function to check and report duplicate fields in a file / table / view
    e1, e2, e3 = build_error_string(i_obj_type, i_curr_row)

    if len(i_curr_row[e3]) != len(set(i_curr_row[e3])):
        e = e2 + ' has duplicate field(s)'
        log_message('e',  e1, '-', str(e))

def lcase_field_check(i_obj_type, i_curr_row, i_iot_exception:bool): # function to check and report non-lowercase fields in a file / table / view  # v7.0
    e1, e2, e3 = build_error_string(i_obj_type, i_curr_row)

    if i_iot_exception == False: # v7.0
        if [x.lower() for x in i_curr_row[e3]] != i_curr_row[e3]:
            e = e2 + ' has field(s) other than lower case'
            log_message('e',  e1, '-', str(e))

def special_char_field_check(i_obj_type, i_curr_row, i_iot_exception:bool): # function to check and report special char fields in a file / table / view # v7.0
    global validchar_list
    global validchar_list_iot # v7.0

    e1, e2, e3 = build_error_string(i_obj_type, i_curr_row)

    for x in i_curr_row[e3]:
        invalid_flag = 0
        for y in x:
            if (y not in validchar_list and i_iot_exception == False) or (y not in validchar_list_iot and i_iot_exception == True): # v7.0
               invalid_flag = 1
        if invalid_flag == 1:
            e = 'field <' + x + '>' + 'in ' + e2 + ' has characters apart from a-z, 0-9, _'
            log_message('e', e1, '-', str(e))



# %%
# accept tdd file as input from user

df_log.drop(df_log.index, inplace=True) # refresh the message log for this block

root = tk.Tk()
root.attributes("-topmost", True)
root.withdraw()
file_tdd = tkfd.askopenfilename(title="Select MDLZ template based TDD review .xlsx file")

if file_tdd == "":
    log_message('e', '-', '-', 'tdd not found / supplied')
else:
    log_message('i', '-', '-', os.path.basename(file_tdd).upper()+' review')

if not df_log.empty: # proceed only if error log has data
    log_sort_index() # sort the log to display errors, warning, info...in that order
    display(df_log) # display the log


# %%
# read and store config master

df_log.drop(df_log.index, inplace=True) # refresh the message log for this block
loc = "C:/projectfiles"
file_cm = loc + "/gcp-config-master.xlsx" # set location from where config master has to be read

try:
    df_cm = pd.read_excel(file_cm, sheet_name="gcp-config-master", keep_default_na=False, dtype={'last_update':str}) # read the config master xlsx
except Exception as e:
    log_message('e','config', '-', str(e)) # log error message if config sheet cannot be read

df_cm["rowkey"] = ( # add 'key' to config sheet df to be used later
    df_cm["region"] # v6.0
    + df_cm["data_entity"]
    + df_cm["entity_suffix"]
    + df_cm["source_system"]
    + df_cm["source_object"]
    + df_cm["landing_project"]
    + df_cm["datalake_project"]
    + df_cm["harmonized_dataset"]
    ).str.lower()

if not df_log.empty: # proceed only if error log has data
    log_sort_index() # sort the log to display errors, warning, info...in that order
    display(df_log) # display the log
    

# %%
# read and process tdd xlsx file (summary worksheet) to dataframe

df_log.drop(df_log.index, inplace=True) # refresh the message log for this block

try:
    df_tdd_full = pd.read_excel(file_tdd, sheet_name=None, keep_default_na=False) # read full xlsx as a dictionary in one go first
except Exception as e:
    log_message('e','tdd', '-', str(e)) # log error message if tdd cannot be read

try:
    df_tdd_summary = df_tdd_full.get('Summary') # get summary sheet
    df_tdd_summary.columns = df_tdd_summary.columns.str.replace(' ', '_').str.lower() # replace <space> if any in column names with _
except Exception as e:
    log_message('e','summary', '-', str(e)) # log error message if summary sheet cannot be read

try: # v8.0
    df_tdd_summary["rowkey"] = ( # add 'key' to summary sheet df to be used later
        df_tdd_summary["gcp"] 
        + df_tdd_summary["data_entity"] 
        + df_tdd_summary["entity_suffix"] 
        + df_tdd_summary["source_system"] 
        + df_tdd_summary["source_object"]
        + df_tdd_summary["landing_project"]
        + df_tdd_summary["data_lake_project"]
        + df_tdd_summary["harmonized_dataset"]
        ).str.lower()
except: # v8.0
    df_tdd_summary["rowkey"] = ( # add 'key' to summary sheet df to be used later # v8.0
        df_tdd_summary["region"] # v8.0
        + df_tdd_summary["data_entity"] # v8.0
        + df_tdd_summary["entity_suffix"] # v8.0
        + df_tdd_summary["source_system"] # v8.0
        + df_tdd_summary["source_object"] # v8.0
        + df_tdd_summary["landing_project"]
        + df_tdd_summary["data_lake_project"]
        + df_tdd_summary["harmonized_dataset"]
        ).str.lower() # v8.0

df_tdd_summary.reset_index()

df_tdd_summary = pd.merge(df_tdd_summary, df_cm[['rowkey','source_object_type']], on ='rowkey', how ='inner') # v7.0


# %%
df_cm_wip = df_cm.loc[df_cm["rowkey"].isin(df_tdd_summary["rowkey"])]
df_cm = df_cm_wip

# %%
# read and process tdd xlsx file (remaining worksheets)

try:
    df_tdd_source = df_tdd_full.get('Source') # get source sheet
    df_tdd_source.columns = df_tdd_source.columns.str.replace(' ', '_').str.lower() # replace <space> if any in column names with _
except Exception as e:
    log_message('e','source', '-', str(e)) # log error message if source sheet cannot be read

try:
    df_tdd_gcs = df_tdd_full.get('GCS') # get gcs sheet
    df_tdd_gcs.columns = df_tdd_gcs.columns.str.replace(' ', '_').str.lower() # replace <space> if any in column names with _
except Exception as e:
    log_message('e','gcs', '-', str(e)) # log error message if gcs sheet cannot be read

try:
    df_tdd_rt = df_tdd_full.get('Raw Table') # get raw table sheet
    df_tdd_rt.columns = df_tdd_rt.columns.str.replace(' ', '_').str.lower() # replace <space> if any in column names with _
except Exception as e:
    log_message('e','raw table', '-', str(e)) # log error message if raw table sheet cannot be read

try:
    df_tdd_rv = df_tdd_full.get('Raw View') # get raw view sheet
    df_tdd_rv.columns = df_tdd_rv.columns.str.replace(' ', '_').str.lower() # replace <space> if any in column names with _
except Exception as e:
    log_message('e','raw view', '-', str(e)) # log error message if raw view sheet cannot be read

try:
    df_tdd_ht = df_tdd_full.get('Harmonized Table') # get harmonized table sheet
    df_tdd_ht.columns = df_tdd_ht.columns.str.replace(' ', '_').str.lower() # replace <space> if any in column names with _
except Exception as e:
    log_message('e','harmonized table', '-', str(e)) # log error message if harmonized table sheet cannot be read

try:
    df_tdd_hv = df_tdd_full.get('Harmonized View') # get harmonized view sheet
    df_tdd_hv.columns = df_tdd_hv.columns.str.replace(' ', '_').str.lower() # replace <space> if any in column names with _
except Exception as e:
    log_message('e','harmonized view', '-', str(e)) # log error message if harmonized view sheet cannot be read

if not df_log.empty: # proceed only if error log has data
    log_sort_index() # sort the log to display errors, warning, info...in that order
    display(df_log) # display the log


# %%
# keep all groupby ready for further use

df_log.drop(df_log.index, inplace=True) # refresh the message log for this block

try:
    try: # v8.0
        df_tdd_summ_gp = df_tdd_summary.groupby( # group summary sheet data for later use 
            ['gcp', 'data_entity', 'entity_suffix', 'source_system', 'source_object'])['source_file'].apply(list).to_frame().reset_index()
    except: # v8.0
        df_tdd_summ_gp = df_tdd_summary.groupby( # group summary sheet data for later use # v8.0
            ['region', 'data_entity', 'entity_suffix', 'source_system', 'source_object'])['source_file'].apply(list).to_frame().reset_index() # v8.0

    df_tdd_summ_ht_gp = df_tdd_summary.groupby( # group summary sheet data (customized for harmonized table read) for later use 
        ['harmonized_dataset', 'harmonized_table']).apply(
            lambda x: [list(x['data_entity']), list(x['raw_dataset']), list(x['raw_view']), list(x['source_object_type'])]).apply(pd.Series).reset_index() # v7.0
    df_tdd_summ_ht_gp.columns =[
        'harmonized_dataset', 'harmonized_table', 'data_entity', 'raw_dataset', 'raw_view', 'source_object_type'] # restate column names # v7.0

    df_tdd_summ_hv_gp = df_tdd_summary.groupby( # group summary sheet data (customized for harmonized view read) for later use 
        ['harmonized_dataset', 'harmonized_view']).apply(
            lambda x: [list(x['data_entity']), list(x['raw_dataset']), list(x['raw_view']), list(x['harmonized_table']), list(x['source_object_type'])]).apply(
                pd.Series).reset_index() # v7.0
    df_tdd_summ_hv_gp.columns =[
        'harmonized_dataset', 'harmonized_view', 'data_entity', 'raw_dataset', 'raw_view', 'harmonized_table', 'source_object_type'] # restate column names # v7.0

except Exception as e:
    log_message('e','summary-1', '-', str(e)) # log error message

try:
    df_tdd_src_gp = df_tdd_source.groupby( # group source sheet data for later use 
        ['source_file']).apply(
            lambda x: [list(x['source_field_name']), list(x['key']), list(x['dim_/_fact']), list(x['source_field_data_type'])]).apply(
                pd.Series).reset_index()
    df_tdd_src_gp.columns =[
        'source_file', 'source_field_name', 'key', 'dim_/_fact', 'source_field_data_type'] # restate column names
except Exception as e:
    log_message('e','summary-2', '-', str(e)) # log error message

try:
    df_tdd_gcs_gp = df_tdd_gcs.groupby( # group gcs sheet data for later use 
        ['gcs_folder', 'gcs_file']).apply(
            lambda x: [list(x['gcs_field_name']), list(x['key']), list(x['dim_/_fact']), list(x['mapping_/_reference_/_calculation'])]).apply(
                pd.Series).reset_index()
    df_tdd_gcs_gp.columns =[
        'gcs_folder', 'gcs_file', 'gcs_field_name', 'key', 'dim_/_fact', 'mapping_/_reference_/_calculation'] # restate column names
except Exception as e:
    log_message('e','summary-3', '-', str(e)) # log error message

try:
    df_tdd_rt_gp = df_tdd_rt.groupby( # group raw table sheet data for later use 
        ['raw_dataset', 'raw_table']).apply(
            lambda x: [list(x['raw_table_field_name']), list(x['key']), list(x['dim_/_fact']), list(x['mapping_/_reference_/_calculation'])]).apply(
                pd.Series).reset_index()
    df_tdd_rt_gp.columns =[
        'raw_dataset', 'raw_table', 'raw_table_field_name', 'key', 'dim_/_fact', 'mapping_/_reference_/_calculation'] # restate column names
except Exception as e:
    log_message('e','summary-4', '-', str(e)) # log error message

try:
    df_tdd_rv_gp = df_tdd_rv.groupby( # group raw view sheet data for later use 
        ['raw_dataset', 'raw_view']).apply(
            lambda x: [list(x['raw_view_field_name']), list(x['key']), list(x['dim_/_fact']), list(x['mapping_/_reference_/_calculation'])]).apply(
                pd.Series).reset_index()
    df_tdd_rv_gp.columns =[
        'raw_dataset', 'raw_view', 'raw_view_field_name', 'key', 'dim_/_fact', 'mapping_/_reference_/_calculation'] # restate column names
except Exception as e:
    log_message('e','summary-5', '-', str(e)) # log error message

try:
    df_tdd_ht_gp = df_tdd_ht.groupby( # group harmonized table sheet data for later use 
        ['harmonized_dataset', 'harmonized_table']).apply(
            lambda x: [list(x['harmonized_table_field_name']), list(x['key']), list(x['dim_/_fact']), list(x['mapping_/_reference_/_calculation'])]).apply(
                pd.Series).reset_index()
    df_tdd_ht_gp.columns =[
        'harmonized_dataset', 'harmonized_table', 'harmonized_table_field_name', 'key', 'dim_/_fact', 'mapping_/_reference_/_calculation'] # restate column names
except Exception as e:
    log_message('e','summary-6', '-', str(e)) # log error message

try:
    df_tdd_hv_gp = df_tdd_hv.groupby( # group harmonized view sheet data for later use 
        ['harmonized_dataset', 'harmonized_view']).apply(
            lambda x: [list(x['harmonized_view_field_name']), list(x['key']), list(x['dim_/_fact']), list(x['mapping_/_reference_/_calculation'])]).apply(
                pd.Series).reset_index()
    df_tdd_hv_gp.columns =[
        'harmonized_dataset', 'harmonized_view', 'harmonized_view_field_name', 'key', 'dim_/_fact', 'mapping_/_reference_/_calculation'] # restate column names
except Exception as e:
    log_message('e','summary-7', '-', str(e)) # log error message

if not df_log.empty: # proceed only if error log has data
    log_sort_index() # sort the log to display errors, warning, info...in that order
    display(df_log) # display the log


# %%
# review summary sheet

df_log.drop(df_log.index, inplace=True) # refresh the message log for this block

# check for duplicate entries in summary
try: # v8.0
    df_tdd_summary_dupl = df_tdd_summary[df_tdd_summary.duplicated( # prepare to find duplicates and store in a df
        ['gcp', 'data_entity', 'entity_suffix', 'source_system', 'source_object', 'harmonized_dataset'])]
except: # v8.0
    df_tdd_summary_dupl = df_tdd_summary[df_tdd_summary.duplicated( # prepare to find duplicates and store in a df # v8.0
        ['region', 'data_entity', 'entity_suffix', 'source_system', 'source_object', 'harmonized_dataset'])] # v8.0
if df_tdd_summary_dupl.shape[0] > 0: # if entry exists in duplicated df
    for idx in df_tdd_summary_dupl.index: # loop thru all the duplicate entries
        e = "entry <" + df_tdd_summary_dupl[idx] + '> is duplicated' # prepare error message about the duplicate entry
        log_message('e','summary', '-', str(e)) # log error message about the duplicate entry

rownum = 0 # reset counter
for idx in df_tdd_summary.index: # loop thru all summary sheet rows
    rownum = idx + 2 # set counter to match excel row num

    curr_row = df_tdd_summary.iloc[idx] # get current row of the summary sheet
    
    try:
        cm_row = df_cm[df_cm["rowkey"] == curr_row["rowkey"]].iloc[0] # get corresponding row in config sheet
        curr_row["source_object_type"] = cm_row["source_object_type"] # v7.0
    except:
        e = "no matching key exists in config sheet" # prepare message corresponding config entry not found
        log_message('e','summary', rownum, str(e)) # log error corresponding config entry not found
        continue # skip current summary sheet row and move to the next one

    # prepare message to inform tdd summary entry current project and owner of the entity
    e = "entity= " + curr_row["data_entity"] + " being modified; current project= " + cm_row["project"] + " ;owner= " + cm_row["requestor"] + " check impact if any"
    log_message('i','summary', rownum, str(e)) # log information about current project and owner

    # check and log error message if tdd summary source file name does not match with config
    e = "source file mismatch with config sheet"
    log_message('e','summary', rownum, str(e)) if(curr_row["source_file"] != cm_row["source_file"]) else None

    # check and log error message if tdd gcs folder name does not match with config
    e = "gcs folder mismatch with config sheet"
    log_message('e','summary', rownum, str(e)) if(curr_row["gcs_folder"] != cm_row["gcs_folder"]) else None

    # check and log error message if tdd gcs file name does not match with config
    e = "gcs file mismatch with config sheet"
    log_message('e','summary', rownum, str(e)) if(curr_row["gcs_file"] != cm_row["gcs_file"]) else None

    # check and log error message if tdd raw dataset name does not match with config
    e = "raw dataset mismatch with config sheet"
    log_message('e','summary', rownum, str(e)) if(curr_row["raw_dataset"] != cm_row["raw_dataset"]) else None
    
    # check and log error message if tdd raw table name does not match with config
    e = "raw table mismatch with config sheet"
    log_message('e','summary', rownum, str(e)) if(curr_row["raw_table"] != cm_row["raw_table"]) else None

    # check and log error message if tdd raw view name does not match with config
    e = "raw view mismatch with config sheet"
    log_message('e','summary', rownum, str(e)) if(curr_row["raw_view"] != cm_row["raw_view"]) else None

    # check and log error message if tdd harmonized dataset name does not match with config
    e = "harmonized dataset mismatch with config sheet"
    log_message('e','summary', rownum, str(e)) if(curr_row["harmonized_dataset"] != cm_row["harmonized_dataset"]) else None
    
    # check and log error message if tdd harmonized table name does not match with config
    e = "harmonized table mismatch with config sheet"
    log_message('e','summary', rownum, str(e)) if(curr_row["harmonized_table"] != cm_row["harmonized_table"]) else None

    # check and log error message if tdd harmonized view name does not match with config
    e = "harmonized view mismatch with config sheet"
    log_message('e','summary', rownum, str(e)) if(curr_row["harmonized_view"] != cm_row["harmonized_view"]) else None

    # check and log error message if tdd summary source file does not have entry in source sheet and name not in ignore list
    try: 
        df_tdd_src_gp[df_tdd_src_gp["source_file"] == curr_row["source_file"]].iloc[0]["source_field_name"] 
    except: 
        e = "source file does not exist in source sheet"
        log_message('e','summary', rownum, str(e)) if curr_row["source_file"].lower() not in ignore_list else None

    # check and log error message if tdd summary gcs folder/file does not have entry in gcs sheet and name not in ignore list
    try: 
        df_tdd_gcs_gp[(df_tdd_gcs_gp["gcs_folder"] == curr_row["gcs_folder"]) & (df_tdd_gcs_gp["gcs_file"] == curr_row["gcs_file"])].iloc[0]["gcs_field_name"] 
    except: 
        e = "gcs folder-file does not exist in gcs sheet"
        log_message('e','summary', rownum, str(e)) if (curr_row["gcs_folder"].lower() not in ignore_list) & (curr_row["gcs_file"].lower() not in ignore_list) else None
    
    # check and log error message if tdd summary raw dataset-table does not have entry in raw table sheet and name not in ignore list
    try: 
        df_tdd_rt_gp[(df_tdd_rt_gp["raw_dataset"] == curr_row["raw_dataset"]) & (df_tdd_rt_gp["raw_table"] == curr_row["raw_table"])].iloc[0]["raw_table_field_name"] 
    except: 
        e = "raw dataset-table does not exist in raw table sheet"
        log_message('e','summary', rownum, str(e)) if (curr_row["raw_dataset"].lower() not in ignore_list) & (curr_row["raw_table"].lower() not in ignore_list) \
        else None

    # check and log error message if tdd summary raw dataset-view does not have entry in raw view sheet and name not in ignore list
    try: 
        df_tdd_rv_gp[(df_tdd_rv_gp["raw_dataset"] == curr_row["raw_dataset"]) & (df_tdd_rv_gp["raw_view"] == curr_row["raw_view"])].iloc[0]["raw_view_field_name"] 
    except: 
        e = "raw dataset-view does not exist in raw table sheet"
        log_message('e','summary', rownum, str(e)) \
        if (curr_row["raw_dataset"].lower() not in ignore_list) & (curr_row["raw_view"].lower() not in ignore_list) else None

    # check and log error message if tdd summary harmonized dataset-table does not have entry in harmonized table sheet and name not in ignore list
    try: 
        df_tdd_ht_gp[(df_tdd_ht_gp["harmonized_dataset"] == curr_row["harmonized_dataset"]) \
        & (df_tdd_ht_gp["harmonized_table"] == curr_row["harmonized_table"])].iloc[0]["harmonized_table_field_name"]
    except: 
        e = "harmonized dataset-table does not exist in harmonized table sheet"
        log_message('e','summary', rownum, str(e)) \
        if (curr_row["harmonized_dataset"].lower() not in ignore_list) & (curr_row["harmonized_table"].lower() not in ignore_list) else None

    # check and log error message if tdd summary harmonized dataset-view does not have entry in harmonized view sheet and name not in ignore list
    try: 
        df_tdd_hv_gp[(df_tdd_hv_gp["harmonized_dataset"] == curr_row["harmonized_dataset"]) & \
        (df_tdd_hv_gp["harmonized_view"] == curr_row["harmonized_view"])].iloc[0]["harmonized_view_field_name"]
    except: 
        e = "harmonized dataset-view does not exist in harmonized view sheet"
        log_message('e','summary', rownum, str(e)) \
        if (curr_row["harmonized_dataset"].lower() not in ignore_list) & (curr_row["harmonized_view"].lower() not in ignore_list) else None

if not df_log.empty: # proceed only if error log has data
    log_sort_index() # sort the log to display errors, warning, info...in that order
    display(df_log) # display the log


# %%
# review source sheet

df_log.drop(df_log.index, inplace=True) # refresh the message log for this block

for idx in df_tdd_src_gp.index: # loop thru all grouped entries in source sheet
    curr_row = df_tdd_src_gp.iloc[idx] # get current row


    # check and log error message if source file entry does not exist in summary sheet
    try: df_tdd_summary[df_tdd_summary['source_file'] == curr_row['source_file']].iloc[0]
    except Exception as e:
        e = curr_row['source_file'] + " mismatch with summary"
        log_message('e','source file', '-', str(e))

    if curr_row['source_file'].lower() in ignore_list: continue # skip current source sheet row and move to the next

    key_check("src", curr_row) # ensure each source file has atleast one key (no value other than X)

    dim_fact_check("src", curr_row) # ensure each source file has atleast one dim, possibly atleast one fact and no value other than dim or fact

    duplicate_field_check("src", curr_row) # ensure no duplicate fields in source file

    # check and log warning message if selective source sheet data types are marked as dim instead of facts
    for i in range(len(curr_row["source_field_data_type"])):
        if (curr_row["source_field_data_type"][i].lower() in (dtyp_list)) & (curr_row["dim_/_fact"][i].lower() != "fact"):
            e = "field <" + curr_row['source_field_name'][i] + "> in " + curr_row['source_file'] + " is marked as dim insead of fact"
            log_message('w','source', '-', str(e))

if not df_log.empty: # proceed only if error log has data
    log_sort_index() # sort the log to display errors, warning, info...in that order
    display(df_log) # display the log


# %%
# review gcs sheet

df_log.drop(df_log.index, inplace=True) # refresh the message log for this block

for idx in df_tdd_gcs_gp.index: # loop thru all grouped entries in gcs sheet
    curr_row = df_tdd_gcs_gp.iloc[idx] # get current row

    try: 
        # check if this gcs file has a match in summary sheet
        summ_row = df_tdd_summary[(df_tdd_summary['gcs_folder'] == curr_row['gcs_folder']) & (df_tdd_summary['gcs_file'] == curr_row['gcs_file'])].iloc[0]

        if curr_row['gcs_file'].lower() in ignore_list: continue # skip further checks as the name is in ignore list (N/A, TBD etc...) and move to next gcs file

        # now peform preceding document (source) match checks
        try: 
            # escape reconciliation check with source for IOT scenario # v7.0
            if not summ_row["source_object_type"] in iot_obj_type_list: # v7.0
                # check whether we can get the corresponding source file for this gcs file
                src_row = df_tdd_src_gp[df_tdd_src_gp['source_file'] == summ_row['source_file']].iloc[0]
                
                # now that we have the source file, check if # of fields in gcs file match with the source file
                if len(df_tdd_gcs_gp.iloc[idx]['gcs_field_name']) != len(src_row['source_field_name']):
                    # issue an error as we expect them to always match
                    e = curr_row['gcs_folder'] + curr_row['gcs_file'] + " field count mismatch with source"
                    log_message('e','gcs file', '-', str(e))
                
                # now that we have the source file, also let's check whether the gcs file fields refer back to the source file
                if any(not item.startswith(src_row['source_file']) for item in curr_row['mapping_/_reference_/_calculation']):
                    # issue an error as we expect them to always match
                    e = curr_row['gcs_folder'] + curr_row['gcs_file'] + ' has fields which do not refer back to source'
                    log_message('e','gcs file', '-', str(e))
        except Exception as e:
            # issue an error as we expect gcs file to always have a corresponding source file
            e = curr_row['gcs_folder'] + curr_row['gcs_file'] + " mismatch with source"
            log_message('e','gcs file', '-', str(e))
    except Exception as e:
        # issue an error as gcs file should always have a corresponding entry in summary sheet
        e = curr_row['gcs_folder'] + curr_row['gcs_file'] + " mismatch with summary"
        log_message('e','gcs file', '-', str(e))
        continue # move to the next gcs file

    key_check("gcs", curr_row) # ensure each gcs file has atleast one key (no value other than X)

    dim_fact_check("gcs", curr_row) # ensure each gcs file has atleast one dim, possibly atleast one fact and no value other than dim or fact
    
    duplicate_field_check("gcs", curr_row) # ensure no duplicate fields in gcs file

if not df_log.empty: # proceed only if error log has data
    log_sort_index() # sort the log to display errors, warning, info...in that order
    display(df_log) # display the log


# %%
# review raw table sheet

df_log.drop(df_log.index, inplace=True) # refresh the message log for this block

for idx in df_tdd_rt_gp.index: # loop thru all grouped entries in gcs sheet
    curr_row = df_tdd_rt_gp.iloc[idx] # get current row

    try: 
        # check if this gcs file has a match in summary sheet
        summ_row = df_tdd_summary[(df_tdd_summary['raw_dataset'] == curr_row['raw_dataset']) & (df_tdd_summary['raw_table'] == curr_row['raw_table'])].iloc[0]

        if curr_row['raw_table'].lower() in ignore_list: continue # skip further checks as the name is in ignore list (N/A, TBD etc...) and move to next raw table

        # now peform preceding document (gcs) match checks
        try: 
            # escape reconciliation check with gcs for IOT scenario # v7.0
            if not summ_row["source_object_type"] in iot_obj_type_list: # v7.0
                # check whether we can get the corresponding gcs file for this raw table
                gcs_row = df_tdd_gcs_gp[(df_tdd_gcs_gp['gcs_folder'] == summ_row['gcs_folder']) & (df_tdd_gcs_gp['gcs_file'] == summ_row['gcs_file'])].iloc[0]

                # check if gcs file is not N/A
                if gcs_row['gcs_file'].lower() not in ignore_list:
                    # now that we have the gcs file, check if # of fields in raw table match with the gcs file
                    if len(df_tdd_rt_gp.iloc[idx]['raw_table_field_name']) != len(gcs_row['gcs_field_name']):
                        # issue an error as we expect them to always match
                        e = curr_row['raw_dataset'] + "." + curr_row['raw_table'] + " field count mismatch with gcs file"
                        log_message('e','raw table', '-', str(e))

                    # now that we have the gcs file, also let's check whether the raw table fields refer back to the gcs file
                    if any(not item.startswith(gcs_row['gcs_folder'] + gcs_row['gcs_file']) for item in curr_row['mapping_/_reference_/_calculation']):
                        # issue an error as we expect them to always match
                        e = curr_row['raw_dataset'] + "." + curr_row['raw_table'] + ' has fields which do not refer back to gcs file'
                        log_message('e','raw table', '-', str(e))
                else:
                    # check whether we can get the corresponding source file for this raw table
                    src_row = df_tdd_src_gp[df_tdd_src_gp['source_file'] == summ_row['source_file']].iloc[0]
                    
                    # now that we have the source file, check if # of fields in raw table match with the source file
                    if len(df_tdd_rt_gp.iloc[idx]['raw_table_field_name']) != len(src_row['source_field_name']):
                        # issue an error as we expect them to always match
                        e = curr_row['raw_dataset'] + "." + curr_row['raw_table'] + " field count mismatch with source file"
                        log_message('w','raw table', '-', str(e))
                    
                    # now that we have the source file, also let's check whether the raw table fields refer back to the source file
                    if any(not item.startswith(src_row['source_file']) for item in curr_row['mapping_/_reference_/_calculation']):
                        # issue an error as we expect them to always match
                        e = curr_row['raw_dataset'] + "." + curr_row['raw_table'] + ' has fields which do not refer back to source file'
                        log_message('w','raw table', '-', str(e))
        except Exception as e:
            # issue an error as we expect raw table to always have a corresponding gcs file
            e = curr_row['raw_dataset'] + "." + curr_row['raw_table'] + " mismatch with gcs"
            log_message('e','raw table', '-', str(e))
    except Exception as e:
        # issue an error as raw table should always have a corresponding entry in summary sheet
        e = curr_row['raw_dataset'] + "." + curr_row['raw_table'] + " mismatch with summary"
        log_message('e','raw table', '-', str(e))
        continue # move to the next raw table

    key_check("rt", curr_row)# ensure each raw table has atleast one key (no value other than X)
    
    dim_fact_check("rt", curr_row)# ensure each raw table has atleast one dim, possibly atleast one fact and no value other than dim or fact
    
    duplicate_field_check("rt", curr_row)# ensure no duplicate fields in raw table
    
    # we need a way to escape lcase and special character rule for iot kind of solutions
    if summ_row["source_object_type"] in iot_obj_type_list: # v7.0
        iot_obj_type_exception = True # v7.0
    else: # v7.0
        iot_obj_type_exception = False # v7.0

    # ensure field names are in lower case
    lcase_field_check("rt", curr_row, iot_obj_type_exception)# ensure field names are in lower case # v7.0

    # ensure field names don't contain special characters
    special_char_field_check("rt", curr_row, iot_obj_type_exception) # v7.0

if not df_log.empty: # proceed only if error log has data
    log_sort_index() # sort the log to display errors, warning, info...in that order
    display(df_log) # display the log


# %%
# review raw view sheet

df_log.drop(df_log.index,inplace=True)

for idx in df_tdd_rv_gp.index:
    curr_row = df_tdd_rv_gp.iloc[idx]

    # ensure raw view entries match with summary sheet
    try: 
        summ_row = df_tdd_summary[(df_tdd_summary['raw_dataset'] == curr_row['raw_dataset']) & (df_tdd_summary['raw_view'] == curr_row['raw_view'])].iloc[0]
    except Exception as e:
        e = curr_row['raw_dataset'] + "." + curr_row['raw_view'] + " mismatch with summary"
        log_message('e','raw view', '-', str(e))
        continue # move to the next raw view

    if curr_row['raw_view'].lower() in ignore_list: continue # move to the next raw view

    # now peform preceding document (raw table) match checks
    try: 
        # check whether we can get the corresponding raw table for this raw view
        rt_row = df_tdd_rt_gp[(df_tdd_rt_gp['raw_dataset'] == summ_row['raw_dataset']) & (df_tdd_rt_gp['raw_table'] == summ_row['raw_table'])].iloc[0]
        # now that we have the raw table, check if # of fields in raw view match with the raw table
        if len(df_tdd_rv_gp.iloc[idx]['raw_view_field_name']) != len(rt_row['raw_table_field_name']):
            # issue an error as we expect them to always match
            e = curr_row['raw_dataset'] + "." + curr_row['raw_view'] + " field count mismatch with raw table"
            log_message('e','raw view', '-', str(e))

        # now that we have the raw table, also let's check whether the view fields refer back to the raw table
        if any(not item.startswith(rt_row['raw_dataset'] + "." + rt_row['raw_table']) for item in curr_row['mapping_/_reference_/_calculation']):
            # issue an error as we expect them to always match
            e = curr_row['raw_dataset'] + "." + curr_row['raw_view'] + ' has fields which do not refer back to raw table'
            log_message('e','raw view', '-', str(e))
    except Exception as e:
            # issue an error as there should have been a corresponding raw table entry according to the summary sheet
            e = curr_row['raw_dataset'] + "." + curr_row['raw_view'] + " mismatch with raw table"
            log_message('e','raw view', '-', str(e))


    # ensure each raw view has atleast one key (no value other than X or blank)
    key_check("rv", curr_row)

    # ensure each raw view has atleast one dim, possibly atleast one fact and no value other than dim or fact
    dim_fact_check("rv", curr_row)

    # ensure no duplicate fields in raw view
    duplicate_field_check("rv", curr_row)

    # we need a way to escape lcase and special character rule for iot kind of solutions
    if summ_row["source_object_type"] in iot_obj_type_list: # v7.0
        iot_obj_type_exception = True # v7.0
    else: # v7.0
        iot_obj_type_exception = False # v7.0

    # ensure field names are in lower case
    lcase_field_check("rv", curr_row, iot_obj_type_exception)# ensure field names are in lower case # v7.0

    # ensure field names don't contain special characters
    special_char_field_check("rv", curr_row, iot_obj_type_exception) # v7.0

if not df_log.empty:
    log_sort_index()
    display(df_log)


# %%
# review harmonized table sheet

df_log.drop(df_log.index,inplace=True)

for idx in df_tdd_ht_gp.index:
    curr_row = df_tdd_ht_gp.iloc[idx]
    if curr_row["harmonized_dataset"].lower() in ignore_list or curr_row["harmonized_table"].lower() in ignore_list: # v9.0
        continue # v9.0

    # ensure harmonized table entries match with summary sheet
    try: 
        # check whether we can get the corresponding summary row for this harmonized table
        summ_row = df_tdd_summ_ht_gp[(df_tdd_summ_ht_gp['harmonized_dataset'] == curr_row['harmonized_dataset']) & (
            df_tdd_summ_ht_gp['harmonized_table'] == curr_row['harmonized_table'])].iloc[0]
        # now that we have summary row, peform preceding document (raw view) match checks
        i = 0
        for x in summ_row['data_entity']:
            if summ_row['raw_view'][i].lower() not in ignore_list:
                try: 
                    # check whether we can get the corresponding raw view for this harmonized table
                    rv_row = df_tdd_rv_gp[(df_tdd_rv_gp['raw_dataset'] == summ_row['raw_dataset'][i]) & (df_tdd_rv_gp['raw_view'] == summ_row['raw_view'][i])].iloc[0]
                    # now that we have the raw view, check if # of fields in harmonized table match with the raw view
                    if len(df_tdd_ht_gp.iloc[idx]['harmonized_table_field_name']) != len(rv_row['raw_view_field_name']):
                        # issue a warning only as we do not expect them to always match
                        e = curr_row['harmonized_dataset'] + "." + curr_row['harmonized_table'] + " field count mismatch with raw view"
                        log_message('w','harmonized table', '-', str(e))

                    # now that we have the raw view, also let's check whether the harmonized table fields refer back to the raw view
                    if any(not (rv_row['raw_dataset'] + "." + rv_row['raw_view']) in item for item in curr_row['mapping_/_reference_/_calculation']):
                        # issue a warning only as we do not expect them to always match
                        e = curr_row['harmonized_dataset'] + "." + curr_row['harmonized_table'] + ' has fields which do not refer back to raw view'
                        log_message('w','harmonized table', '-', str(e))
                except Exception as e:
                    # issue an error as there should have been a corresponding raw view entry according to the summary sheet
                    e = curr_row['harmonized_dataset'] + "." + curr_row['harmonized_table'] + " mismatch with raw view"
                    log_message('e','harmonized table', '-', str(e))
            i += 1
    except Exception as e:
        e = curr_row['harmonized_dataset'] + "." + curr_row['harmonized_table'] + " mismatch with summary"
        log_message('e','harmonized table', '-', str(e))
        continue # move to the next harmonized table

    if curr_row['harmonized_table'].lower() in ignore_list: continue # move to the next harmonized table

    # ensure each harmonized table has atleast one key (no value other than X or blank)
    key_check("ht", curr_row)

    # ensure each harmonized table has atleast one dim, possibly atleast one fact and no value other than dim or fact
    dim_fact_check("ht", curr_row)

    # ensure no duplicate fields in harmonized table
    duplicate_field_check("ht", curr_row)

    # we need a way to escape lcase and special character rule for iot kind of solutions
    iot_obj_type_exception = False
    for x in summ_row["source_object_type"]:
        if x in iot_obj_type_list:
            iot_obj_type_exception = True

    # ensure field names are in lower case
    lcase_field_check("ht", curr_row, iot_obj_type_exception)# ensure field names are in lower case # v7.0

    # ensure field names don't contain special characters
    special_char_field_check("ht", curr_row, iot_obj_type_exception) # v7.0

if not df_log.empty:
    log_sort_index()
    display(df_log)


# %%
# review harmonized view sheet

df_log.drop(df_log.index,inplace=True)

for idx in df_tdd_hv_gp.index:
    curr_row = df_tdd_hv_gp.iloc[idx]

    if curr_row["harmonized_dataset"].lower() in ignore_list or curr_row["harmonized_view"].lower() in ignore_list: # v9.0
        continue # v9.0

    # ensure harmonized view entries match with summary sheet
    try: 
        # check whether we can get the corresponding summary row for this harmonized view
        summ_row = df_tdd_summ_hv_gp[(df_tdd_summ_hv_gp['harmonized_dataset'] == curr_row['harmonized_dataset']) & (
            df_tdd_summ_hv_gp['harmonized_view'] == curr_row['harmonized_view'])].iloc[0]
        # now that we have summary row, peform preceding document (raw view if harmonized table is not available else harmonized table) match checks
        i = 0
        for x in summ_row['data_entity']:
            if summ_row['harmonized_table'][i].lower() not in ignore_list:
                try: 
                    # check whether we can get the corresponding harmonized table for this harmonized view
                    ht_row = df_tdd_ht_gp[(df_tdd_ht_gp['harmonized_dataset'] == curr_row['harmonized_dataset']) & (
                        df_tdd_ht_gp['harmonized_table'] == summ_row['harmonized_table'][i])].iloc[0]
                    # now that we have the harmonized table, check if # of fields in harmonized view match with the harmonized table
                    if len(df_tdd_hv_gp.iloc[idx]['harmonized_view_field_name']) != len(ht_row['harmonized_table_field_name']):
                        # issue an error as we do expect them to always match
                        e = curr_row['harmonized_dataset'] + "." + curr_row['harmonized_view'] + " field count mismatch with harmonized table"
                        log_message('e','harmonized view', '-', str(e))

                    # now that we have the harmonized table, also let's check whether the harmonized view fields refer back to the harmonized table
                    if any(not item.startswith(ht_row['harmonized_dataset'] + "." + ht_row['harmonized_table']) for item in curr_row['mapping_/_reference_/_calculation']):
                        # issue an error as we expect them to always match
                        e = curr_row['harmonized_dataset'] + "." + curr_row['harmonized_view'] + ' has fields which do not refer back to harmonized table'
                        log_message('e','harmonized view', '-', str(e))
                except Exception as e:
                    # issue an error as there should have been a corresponding harmonized table entry according to the summary sheet
                    e = curr_row['harmonized_dataset'] + "." + curr_row['harmonized_view'] + " mismatch with harmonized table"
                    log_message('e','harmonized view', '-', str(e))
            else:
                if summ_row['raw_view'][i].lower() not in ignore_list:
                    try: 
                        # check whether we can get the corresponding raw view for this harmonized view
                        rv_row = df_tdd_rv_gp[(df_tdd_rv_gp['raw_dataset'] == summ_row['raw_dataset'][i]) & (df_tdd_rv_gp['raw_view'] == summ_row['raw_view'][i])].iloc[0]
                        # now that we have the raw view, check if # of fields in harmonized view match with the raw view
                        if len(df_tdd_hv_gp.iloc[idx]['harmonized_view_field_name']) != len(rv_row['raw_view_field_name']):
                            # issue an error as we expect them to always match
                            e = curr_row['harmonized_dataset'] + "." + curr_row['harmonized_view'] + " field count mismatch with raw view"
                            log_message('e','harmonized view', '-', str(e))

                        # now that we have the raw view, also let's check whether the harmonized view fields refer back to the raw view
                        if any(not item.startswith(rv_row['raw_dataset'] + "." + rv_row['raw_view']) for item in curr_row['mapping_/_reference_/_calculation']):
                            # issue an error as we expect them to always match
                            e = curr_row['harmonized_dataset'] + "." + curr_row['harmonized_view'] + ' has fields which do not refer back to raw view'
                            log_message('e','harmonized view', '-', str(e))
                    except Exception as e:
                        # issue an error as there should have been a corresponding raw view entry according to the summary sheet
                        e = curr_row['harmonized_dataset'] + "." + curr_row['harmonized_view'] + " mismatch with raw view"
                        log_message('e','harmonized table', '-', str(e))
            i += 1
    except Exception as e:
        e = curr_row['harmonized_dataset'] + "." + curr_row['harmonized_view'] + " mismatch with summary"
        log_message('e','harmonized view', '-', str(e))
        continue # move to the next harmonized view

    if curr_row['harmonized_view'].lower() in ignore_list: continue # move to the next harmonized view

    # ensure each harmonized table has atleast one key (no value other than X or blank)
    key_check("hv", curr_row)

    # ensure each harmonized table has atleast one dim, possibly atleast one fact and no value other than dim or fact
    dim_fact_check("hv", curr_row)

    # ensure no duplicate fields in harmonized table
    duplicate_field_check("hv", curr_row)

    # we need a way to escape lcase and special character rule for iot kind of solutions
    iot_obj_type_exception = False
    for x in summ_row["source_object_type"]:
        if x in iot_obj_type_list:
            iot_obj_type_exception = True

    # ensure field names are in lower case
    lcase_field_check("hv", curr_row, iot_obj_type_exception)# ensure field names are in lower case # v7.0

    # ensure field names don't contain special characters
    special_char_field_check("hv", curr_row, iot_obj_type_exception) # v7.0

if not df_log.empty:
    log_sort_index()
    display(df_log)



