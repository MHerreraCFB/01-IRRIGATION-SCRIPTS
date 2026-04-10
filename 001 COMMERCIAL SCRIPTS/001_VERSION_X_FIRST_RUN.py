### PART 001: IMPORT USAGE TABLES, JOIN TO COMMERCIAL SHAPES, AND EXPORT MONTHLY TABLES TO GDB
import arcpy
import re
import os
import pandas as pd
import numpy as np
import xlsxwriter
from openpyxl import load_workbook
import datetime
import time
from time import localtime, strftime
print("SCRIPT STARTED AT " + strftime("%m/%d/%Y %H:%M:%S", localtime()))

DATE = strftime("%d%b%y", localtime()).upper()
arcpy.ClearWorkspaceCache_management()
arcpy.env.overwriteOutput = True

MONTH_LIST = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']


### CHANGE CURRENT MONTH, USE 3 LETTER ABBREVIATION ###

CURRENT_MONTH = "JAN" ####################################### REQUIRES USER INPUT TO CHANGE, USE 3-LETTER MONTH KEY FOR CURRENT MONTH YOU ARE RUNNING
DICT={'JAN':'1',
'FEB':'2',
'MAR':'3',
'APR':'4',
'MAY':'5',
'JUN':'6',
'JUL':'7',
'AUG':'8',
'SEP':'9',
'OCT':'10',
'NOV':'11',
'DEC':'12'}

for m in MONTH_LIST:
    cols = ['Account','Account Prefix', 'Customer Name', 'Customer Address', 'Usage', 'Date', 'Base', 'PROVIDER']
    df = pd.DataFrame(columns = cols)
    print(m)
    MONTH = m
    YEAR = "26"
    YEAR2 = "25"
    sheet_month = DICT[m]
    var_dict = {1:"df1",
            2:"df2",
            3:"df3",
            4:"df4"}
    
    ## LOOP THRU RAW/RECEIVED EXCEL FILES, CONVERT TO DF AND APPEND TO ONE DF ###
    directory = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\01 MONTHLY RESULTS\COMMERCIAL_TABLES" + "\\" + CURRENT_MONTH + YEAR + "\\" + "2026 Water Usage"
    count = 0
    rows= []
    time.sleep(10)
    for filename in os.listdir(directory):
            file = os.path.join(directory, filename)
            print("Printing file name")
            print(file)
            if os.path.basename(file).startswith("~"):
                continue
            count = count +1
            print("Count")
            print(count)
            df_sheets = pd.read_excel(file, sheet_name=None, engine="openpyxl")
            time.sleep(3)
            #Using keys() method of data frame
            sheet_list = list(df_sheets.keys())
            worksheetName1 = str(sheet_month) + "-" + YEAR
            worksheetName2 = str(sheet_month) + "-20" + YEAR
            worksheetName1b = str(sheet_month) + "-" + YEAR2
            worksheetName2b = str(sheet_month) + "-20" + YEAR2
            time.sleep(5)

            valid_names = {worksheetName1, worksheetName2, worksheetName1b, worksheetName2b}

            print("Printing sheet list")
            print(sheet_list)
            for sheet in sheet_list:
                print(f"Processing sheet: {sheet}")
                read_sheet = pd.read_excel(file, sheet_name=sheet)

                 # Skip empty sheets
                if read_sheet.empty or len(read_sheet.columns) == 0:
                    print(f"Sheet {sheet} is empty, skipping...")
                    continue
                print(sheet)
                # Skip sheet not in current month being run
                if sheet not in valid_names:
                    print(f"Skipping sheet (not target month): {sheet}")
                    continue

                if (worksheetName1 in sheet and sheet != ("1" + worksheetName1)):
                    print(filename, worksheetName1)
                    if ('FWCA' not in filename and 'SEWWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'SEWWCA'
                    elif ('FWCA' not in filename and 'MWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'MWCA'
                    elif ('FWCA' not in filename and 'GPWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'GPWCA'
                    elif ('FWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'FWCA'
                elif (worksheetName2 in sheet and sheet != ("1" + worksheetName2)):
                    print(filename, worksheetName2)
                    if ('FWCA' not in filename and 'SEWWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'SEWWCA'
                    elif ('FWCA' not in filename and 'MWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'MWCA'
                    elif ('FWCA' not in filename and 'GPWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'GPWCA'
                    elif ('FWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'FWCA'
                elif (worksheetName1b in sheet and sheet != ("1" + worksheetName1b)):
                    print(filename, worksheetName1b)
                    if ('FWCA' not in filename and 'SEWWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'SEWWCA'
                    elif ('FWCA' not in filename and 'MWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'MWCA'
                    elif ('FWCA' not in filename and 'GPWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'GPWCA'
                    elif ('FWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'FWCA'
                elif (worksheetName2b in sheet and sheet != ("1" + worksheetName2b)):
                    print(filename, worksheetName2b)
                    if ('FWCA' not in filename and 'SEWWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'SEWWCA'
                    elif ('FWCA' not in filename and 'MWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'MWCA'
                    elif ('FWCA' not in filename and 'GPWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'GPWCA'
                    elif ('FWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'FWCA'
                else: pass
                time.sleep(10)
                rows = []
    for k in var_dict:
        if type(var_dict[k]) != str:
            rows.append(var_dict[k])  # Collect each row
            time.sleep(3)
    # Concatenate all at once
        print("Rows:", rows)
        if not rows:
            print("No rows collected for this file — skipping this file.")
            rmd_clean = pd.DataFrame()
            continue
        df = pd.concat(rows, ignore_index=True)
    #print(df)
        df.rename(columns=lambda x: x.strip(), inplace = True)

    ### IMPORT TABLE WITH RMD_USAGE, MERGE TO COMMERCIAL SHAPES, HANDLE NON-MATCHES ###
        rmd_table = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\01 MONTHLY RESULTS\COMMERCIAL_TABLES" + "\\" + "MSI102.xlsx"
        rmd = pd.read_excel(rmd_table)
        rmd.columns = rmd.columns.str.strip().str.replace('\u00A0', ' ')
        print("MSI has incoming columns:", rmd.columns.tolist())
        rmd.rename(columns = {"Permitted Gallons Per Month":"RMD_USAGE"}, inplace = True)

        ### NON-MATCHES FROM SOD TABLE WILL DROP OUT - NOT USEFUL WITHOUT USAGE ###

        rmd_merge = pd.merge(df, rmd, how='inner', on='Account Prefix', suffixes=('_1', '_2'))

        rmd_merge["Account"] = rmd_merge["Account"].astype(str).str.strip()
        rmd_merge.columns.duplicated()
        print("Merge dataframe has columns:")
        print(rmd_merge.columns)
        rmd_clean = rmd_merge.loc[:, ~rmd_merge.columns.duplicated()]
        rmd_clean = rmd_merge.sort_values(by=['Account'])

        import numpy as np
        # Strip column names
        rmd_clean.rename(columns=lambda x: x.strip(), inplace=True)
       
        ### EXPIRED: GROUP BY RES_ID TO EFFECTIVELY DISSOLVE AND SUM USAGE AND BASE VALUE FIELDS, WHILE CONCATENATING THE TWO STRINGS FOR NAME AND ACCOUNT FIELDS (lIST NEEDED TO RETAIN ORDER) ###                                                                   
        ### REASON FOR EXPIRATION: NO DUPLICATES OF ACCOUNT NUMBER IN COMMERCIAL DATA
        ### SEARCHING FOR DUPLICATES IN ACCOUNT COLUMN TO VERIFY EXPIRATION LOGIC

        if rmd_clean['Account'].duplicated().any():
            print("Warning: duplicates found in ACCOUNT field!")

        ### REMOVE QUOTES AND BRACKETS FROM NAME AND ACCOUNT ###
        rmd_clean["Account"] = rmd_clean["Account"].astype(str).str.replace(r"[\[\]\"']", "", regex=True).replace(r'^\s*$', np.nan, regex=True)
        rmd_clean["Account"] = rmd_clean["Account"].astype(str).str.strip("\"")
        rmd_clean["Customer Name"] = rmd_clean["Customer Name"].astype(str).str.replace(r"[\[\]\"']", "", regex=True).replace(r'^\s*$', np.nan, regex=True)
        rmd_clean["Customer Name"] = rmd_clean["Customer Name"].astype(str).str.strip("\"")
        # df["Account"] = df["Account"].astype(str).str.replace(", ", "/")
        rmd_clean["Customer Name"] = rmd_clean["Customer Name"].astype(str).str.replace(", ", "/")

        
        ### ADD EXTRA FIELDS, CALCULATE ###
        rmd_clean.rename(columns = {"Usage":"Irrigation_Usage"}, inplace = True)
        rmd_clean[MONTH + '_USAGE'] = np.nan
        rmd_clean[MONTH + '_RMD'] = rmd_clean['RMD_USAGE']
        rmd_clean[MONTH + '_PCT'] = np.nan


        ### UPDATE USAGE AND PCT FOR THAT MONTH ###
        rmd_clean = rmd_clean.drop_duplicates(subset="Account", keep="first")
        ### REMOVE DUPLICATE COLUMNS
        rmd_clean = rmd_clean.loc[:,~rmd_clean.columns.duplicated()]
        print(rmd_clean.columns[rmd_clean.columns.duplicated()])
        MONTH_USAGE = MONTH + "_USAGE"
        MONTH_PCT = MONTH + "_PCT"
        MONTH_RMD = MONTH + "_RMD"
        rmd_clean[MONTH_USAGE] = rmd_clean["Irrigation_Usage"]
        rmd_clean[MONTH_PCT] = rmd_clean["Irrigation_Usage"]/rmd_clean[MONTH_RMD]*100
        rmd_clean[MONTH_PCT] = (
            rmd_clean[MONTH_PCT]
                .replace([np.inf, -np.inf], 0)
                .fillna(0)
            )

        #DROP UNNEEDED FIELDS
        fields_to_drop = ["Meter_No", "Billing__1", "Billing__2"]
        rmd_clean = rmd_clean.drop(columns=fields_to_drop, errors='ignore')
        print(rmd_clean)
        ### CREATE FOLDER FOR TABLES (IF NOT YET EXISTING), WRITE MERGED DATAFRAME TO EXCEL ###
    if not rmd_clean.empty:
        parent_dir = r"A:\TEST_LOCATION\03 PROGRAMMING\0008_IRRIGATION_USAGE\002_COMMERCIAL\OUTPUT_TABLES"
        output_folder = os.path.join(parent_dir, CURRENT_MONTH + YEAR)
            
            ###CREATE FOLDER IF IT DOES NOT EXIST
        if not os.path.exists(output_folder):
                os.mkdir(output_folder)
                
                # Create full Excel file path
        excel_file_path = os.path.join(output_folder, f"COMMERCIAL_IRRIGATION_USAGE_{MONTH}{YEAR}.xlsx")
        print("excel_file_path:", excel_file_path) 

            ###WRITE DATAFRAME TO EXCEL 
        try:   
            with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
                rmd_clean.to_excel(writer, MONTH + YEAR, index=False, startrow=0, startcol=0)
        except Exception as e:
            print("WRITE ERROR:", e)

        print("File exists after writing:", os.path.isfile(excel_file_path))

        time.sleep(10)
            
        output_table = excel_file_path.replace(".xlsx", "")
            
        import arcpy
        arcpy.ClearWorkspaceCache_management()   

        ### CONVERT TABLE TO GDB ###
        # table = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\01 DATA\00 GIS\IRRIGATION.gdb\D14_MATCHES"
        gdb_path = os.path.join(r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\01 MONTHLY RESULTS\GEODATABASE",
                            f"{CURRENT_MONTH}{YEAR}.gdb")
        out_table = os.path.join(gdb_path, f"COMMERCIAL_IRRIGATION_USAGE_{MONTH}{YEAR}")

        arcpy.conversion.ExcelToTable(excel_file_path, out_table)
        arcpy.ClearWorkspaceCache_management()
    else:
        print(f"No data in dataframe for {MONTH}{YEAR}, skipping exporting operations. ")