"""
===============================================================================
SCRIPT NAME:    001_PREP_MONTHLY_USAGE_DATA.py (COMMERCIAL)

AUTHOR:         Moises Herrera
CREATED ON:     2025-10-01 (YYYY-MM-DD)
LAST UPDATED:   2026-04-08
VERSION:        1.0.0

DESCRIPTION:
    Prepares irrigation data on commercial areas for Irrigation Dashboard

USAGE:
    Hit run or run in debug mode

REQUIREMENTS:
    - Python version: 3.11.8
    - Dependencies: arcpy, os, pandas, numpy, openpyxl, time

INPUTS:
    Monthly irrigation data excel sheets from MDS system.

OUTPUTS:
    Creates a new table of aggregated and prepared data fields for the current month

NOTES:
    Part one in a series of four, and a one and a half.
    Export out the most up to date commercial shapes to A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\00 GEODATABASE\IRRIGATION_USAGE.gdb

COMMON BUGS AND SOLUTIONS: File is not a zip file. Change Excel type to correct type.

CHANGELOG:
    YYYY-MM-DD - <Author>: <Another change>
    YYYY-MM-DD - <Author>: <Another change>
    YYYY-MM-DD - <Author>: <Another change>

===============================================================================
"""

### PART 001: IMPORT USAGE TABLES, JOIN TO COMMERCIAL SHAPES, AND EXPORT MONTHLY TABLES TO GDB
import arcpy
import os
import pandas as pd
import numpy as np
from openpyxl import load_workbook
from datetime import datetime, timedelta
from time import localtime, strftime
print("SCRIPT STARTED AT " + strftime("%m/%d/%Y %H:%M:%S", localtime()))

DATE = strftime("%d%b%y", localtime()).upper()
arcpy.ClearWorkspaceCache_management()
arcpy.env.overwriteOutput = True

CURRENT_MONTH = (datetime.now() - timedelta(days=30)).strftime("%b").upper()

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


cols = ['Account','Account Prefix', 'Customer Name', 'Customer Address', 'Usage', 'Date', 'Base', 'PROVIDER']
df = pd.DataFrame(columns = cols)

YEAR = str(int(datetime.now().strftime("%y")))
YEAR2 = str(int((datetime.now() - timedelta(360)).strftime("%y")))
sheet_month = DICT[CURRENT_MONTH]
var_dict = {1:"df1",
            2:"df2",
            3:"df3",
            4:"df4"}
    
## LOOP THRU RAW/RECEIVED EXCEL FILES, CONVERT TO DF AND APPEND TO ONE DF ###
directory = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\00 SUPPORT\TABLES_RECVD\COMMERCIAL" + "\\" + CURRENT_MONTH + YEAR + "\\" + "2026 Water Usage"
count = 0
rows= []
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
            #Using keys() method of data frame
            sheet_list = list(df_sheets.keys())
            worksheetName2 = str(sheet_month) + "-20" + YEAR
            worksheetName2b = str(sheet_month) + "-20" + YEAR2
   
            valid_names = {worksheetName2, worksheetName2b}

            print("Printing sheet list")
            print(sheet_list)
            for sheet in sheet_list:
                read_sheet = pd.read_excel(file, sheet_name=sheet)
                 # Skip empty sheets
                if read_sheet.empty or len(read_sheet.columns) == 0:
                    continue
                print(sheet)
                # Skip sheet not in current month being run
                if sheet not in valid_names:
                    continue
                var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                rows = []
for k in var_dict:
        if type(var_dict[k]) != str:
            rows.append(var_dict[k])  # Collect each row
            
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
        irrigation_ciac = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\00 GEODATABASE\IRRIGATION_USAGE.gdb\IRRIGATION_CIAC"
        fields = ["ACC_PREFIX", "PROVIDER", "Type", "Permitted_Gallons_Per_Month"]
        data = [row for row in arcpy.da.SearchCursor(irrigation_ciac, fields)]

        rmd = pd.DataFrame(data, columns=fields)
        rmd.columns = rmd.columns.str.strip().str.replace('\u00A0', ' ')
        rmd.rename(columns = {"Permitted_Gallons_Per_Month":"RMD_USAGE"}, inplace = True)
        rmd.rename(columns = {"ACC_PREFIX":"Account Prefix"}, inplace = True)

        rmd_merge = pd.merge(df, rmd, how='inner', on='Account Prefix', suffixes=('_1', '_2'))
        rmd_merge.rename(columns = {"Usage":"Irrigation_Usage"}, inplace = True)
        rmd_merge["Account"] = rmd_merge["Account"].astype(str).str.strip()
        rmd_merge.columns.duplicated()
        print("Merge dataframe has columns:")
        print(rmd_merge.columns)
        rmd_clean = rmd_merge.loc[:, ~rmd_merge.columns.duplicated()]
        rmd_clean = rmd_merge.sort_values(by=['Account'])

        import numpy as np
        # Strip column names
        rmd_clean.rename(columns=lambda x: x.strip(), inplace=True)

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
        rmd_clean[CURRENT_MONTH + '_USAGE'] = np.nan
        rmd_clean[CURRENT_MONTH + '_RMD'] = rmd_clean['RMD_USAGE']
        rmd_clean[CURRENT_MONTH + '_PCT'] = np.nan


        ### UPDATE USAGE AND PCT FOR THAT MONTH ###
        rmd_clean = rmd_clean.drop_duplicates(subset="Account", keep="first")
        ### REMOVE DUPLICATE COLUMNS
        rmd_clean = rmd_clean.loc[:,~rmd_clean.columns.duplicated()]
        print(rmd_clean.columns[rmd_clean.columns.duplicated()])
        MONTH_USAGE = CURRENT_MONTH + "_USAGE"
        MONTH_PCT = CURRENT_MONTH + "_PCT"
        MONTH_RMD = CURRENT_MONTH + "_RMD"
        rmd_clean[MONTH_USAGE] = rmd_clean["Irrigation_Usage"]
        rmd_clean[MONTH_PCT] = rmd_clean["Irrigation_Usage"]/rmd_clean[MONTH_RMD]*100
        rmd_clean[MONTH_PCT] = (
            rmd_clean[MONTH_PCT]
                .replace([np.inf, -np.inf], 0)
                .fillna(0)
            )
        ### CREATE FOLDER FOR TABLES (IF NOT YET EXISTING), WRITE MERGED DATAFRAME TO EXCEL ###
if not rmd_clean.empty:
        parent_dir = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\01 MONTHLY RESULTS\COMMERCIAL_TABLES"
        output_folder = os.path.join(parent_dir, CURRENT_MONTH + YEAR)
            
            ###CREATE FOLDER IF IT DOES NOT EXIST
        if not os.path.exists(output_folder):
                os.mkdir(output_folder)
                
                # Create full Excel file path
        excel_file_path = os.path.join(output_folder, f"COMMERCIAL_IRRIGATION_USAGE_{CURRENT_MONTH}{YEAR}.xlsx")
        print("excel_file_path:", excel_file_path) 

            ###WRITE DATAFRAME TO EXCEL 
        try:   
            with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
                rmd_clean.to_excel(writer, CURRENT_MONTH + YEAR, index=False, startrow=0, startcol=0)
        except Exception as e:
            print("WRITE ERROR:", e)

        print("File exists after writing:", os.path.isfile(excel_file_path))
else:
        print(f"No data in dataframe for {CURRENT_MONTH}{YEAR}, skipping exporting operations. ")