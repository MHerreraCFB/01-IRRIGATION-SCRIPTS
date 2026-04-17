"""
===============================================================================
SCRIPT NAME:    001_PREP_MONTHLY_USAGE_DATA.py

AUTHOR:         Nathan Shull
CREATED ON:     2023-11-01 (YYYY-MM-DD)
LAST UPDATED:   2025-07-01 (YYYY-MM-DD)
VERSION:        1.2.0 (e.g., 1.0.0)

DESCRIPTION:
    Prepares data for irrigation usage dashboards

USAGE:
    Hit run or run in debug mode

REQUIREMENTS:
    - Python version: 3.11.8
    - Dependencies: arcpy, os, pandas, numpy, openpyxl, time

INPUTS:
    Monthly irrigation data excel sheets from Jamie/MDS system.

OUTPUTS:
    Creates a new table of aggregated and prepared data fields for the current month

NOTES:
    Part one in a series of four, and a one and a half. Must change the month to current month before running.
    Acculumate new closing date to the complete closing date file for the complete file, open last months and append last months to the current month

CHANGELOG:
    2025-07-01 - Moises Herrera: Removed iterative run on all months using all excel sheets from previous month. Instead, I designed it to only run on current month. 
    and I created 001_5 (1.5) which reuses results from previous month to prepare the final result. This reduces the total time of running the script from 8 hours to 1 hour
    with the same consistent results. 
    2025-09-04 - Moises Herrera: There was a change in SEWWCA data formatting that we receive monthly. We have in this data set (excel) the Unit column with multi data types, and those ranging
    in 718 - 740 approximately are integer values with no S prefix. The result is 2000 homesites not matched in Res-ID through additions from the no_unit_sub portion of the script. 
    To counteract the change in data formatting, we make all Unit column a string, and all no_unit_sub become a string as well. 
    2025-10-02 - Moises Herrera: Client prepares excel data consistent with previous month. Reverting back to code as it was before 2025-09-04
    YYYY-MM-DD - <Author>: <Another change>
    YYYY-MM-DD - <Author>: <Another change>
===============================================================================
"""

### PART 001: IMPORT USAGE TABLES AND EXPORT PREPARED TABLE TO EXCEL
### BEFORE RUNNING SCRIPTS EXPORT OUT NEW HOMESITE FROM DEV_RESIDENTIAL
import arcpy
import re
import os
import arcpy.management
import pandas as pd
import numpy as np
import xlsxwriter
from openpyxl import load_workbook
from datetime import datetime, timedelta
import time
from time import localtime, strftime
from IRRIGATION_PREPARE_PACKAGE import prepare_dataframe

print("SCRIPT STARTED AT " + strftime("%m/%d/%Y %H:%M:%S", localtime()))

closing_date_complete_table = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\00 SUPPORT\TABLES_RECVD\RESIDENTIAL\CLOSING_DATE_COMPLETE\MSI100.xlsx"
cdt = pd.read_excel(closing_date_complete_table, 0)


# RENAME COLUMNS #
cdt.rename(columns = {"HOMESITE_ALL_KEYS":"RES_ID_CDT"}, inplace = True)
cdt.rename(columns = {"PHYSICAL_CLOSING_DATE":"CLOSING_DATE"}, inplace = True)

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

cols = ['Account','Account Prefix', 'Name', 'Address', 'Usage', 'Base', 'Unit', 'Lot', 'Unit-Lot', 'Date', 'PROVIDER']
df = pd.DataFrame(columns = cols)
YEAR = str(int(datetime.now().strftime("%y")))
YEAR2 = str(int((datetime.now() - timedelta(360)).strftime("%y")))
sheet_month = DICT[CURRENT_MONTH]
var_dict = {1:"df1",
            2:"df2",
            3:"df3",
            4:"df4"}
    
### LOOP THRU RAW/RECEIVED EXCEL FILES, CONVERT TO DF AND APPEND TO ONE DF ###
directory = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\00 SUPPORT\TABLES_RECVD\RESIDENTIAL" + "\\" + CURRENT_MONTH + YEAR + "\\" + "MOD"
count = 0
rows= []
for filename in os.listdir(directory):
        count = count +1
        file = os.path.join(directory, filename)
        df_sheets = pd.read_excel(file, None)
        time.sleep(1)
        #Using keys() method of data frame
        sheet_list = list(df_sheets.keys())
        worksheetName2 = str(sheet_month) + "-20" + YEAR
        worksheetName2b = str(sheet_month) + "-20" + YEAR2
        print(sheet_list)

        valid_names = {worksheetName2, worksheetName2b}
        for sheet in sheet_list:
            print(sheet)
            if sheet not in valid_names:
                print(f"Skipping sheet (not target month): {sheet}")
                continue
            elif (worksheetName2 in sheet):
                print(filename, worksheetName2)
                if ('SEWWCA' in filename):
                    var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                    var_dict[count]["PROVIDER"] = 'SEWWCA'
                elif ('MWCA' in filename):
                    var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                    var_dict[count]["PROVIDER"] = 'MWCA'
                elif ('GPWCA' in filename):
                    var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                    var_dict[count]["PROVIDER"] = 'GPWCA'
                elif ('FWCA' in filename):
                    var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                    var_dict[count]["PROVIDER"] = 'FWCA'
            elif (worksheetName2b in sheet):
                    print(filename, worksheetName2b)
                    if ('SEWWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'SEWWCA'
                    elif ('MWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'MWCA'
                    elif ('GPWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'GPWCA'
                    elif ('FWCA' in filename):
                        var_dict[count] = pd.read_excel(file, sheet_name= sheet)
                        var_dict[count]["PROVIDER"] = 'FWCA'
            else: pass

for k in var_dict:
        if type(var_dict[k]) != str:
            rows.append(var_dict[k])  # Collect each row

    # Concatenate all at once
        print("Rows:", rows)
        df = pd.concat(rows, ignore_index=True)

        df.to_csv("frame_after_excel_reading.csv", index=False)

    # Prepare Dataframe using a support python file
        df_ready = prepare_dataframe(df, cdt)

        ### CREATE FOLDER FOR TABLES (IF NOT YET EXISTING), WRITE MERGED DATAFRAME TO EXCEL ###
        parent_dir = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\01 MONTHLY RESULTS\RESIDENTIAL_TABLES"
        output_folder = os.path.join(parent_dir, CURRENT_MONTH + YEAR)
        if not os.path.exists(output_folder):
            os.mkdir(output_folder)
        writer = pd.ExcelWriter(parent_dir + "\\"+ CURRENT_MONTH + YEAR + "\\" + "IRRIGATION_USAGE_" + CURRENT_MONTH + YEAR + ".xlsx", engine='xlsxwriter')
        df_ready.to_excel(writer, CURRENT_MONTH + YEAR,index=False, startrow=0 , startcol=0)
        writer.close()