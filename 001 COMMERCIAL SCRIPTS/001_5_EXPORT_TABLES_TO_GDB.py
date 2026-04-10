"""
===============================================================================
SCRIPT NAME:    001_5_EXPORT_TABLES_TO_GDB.py

AUTHOR:         Moises Herrera
CREATED ON:     2025-07-01 (YYYY-MM-DD)
LAST UPDATED:   2025-07-01 (YYYY-MM-DD)
VERSION:        1.0.0.0 (e.g., 1.0.0)

DESCRIPTION:
    Exports excel sheets as tables in a geodatabase that is used to merge homesites to the aggregated data

USAGE:
    Run button, run in debug mode

REQUIREMENTS:
    - Python version: 3.11.8
    - Dependencies: arcpy, os, arcpy.management, pandas, numpy, xlsxwriter, re, datetime, time

INPUTS:
    Excel sheets of aggregated and prepared irrigation data found r"A:\TEST_LOCATION\03 PROGRAMMING\0008_IRRIGATION_USAGE\OUTPUT_TABLES" + "\\"+ CURRENT_MONTH + YEAR + "\\" + "IRRIGATION_USAGE_" + MONTH + YEAR + ".xlsx"

OUTPUTS:
    Tables in a geodatabase

NOTES:
    Step one point five in a four part series, must change month to current month before running, must import excel sheets from previous months into filepath before running,
    must create a gdb at filepath for current month before running. This script was created as an addition to the four part series to reduce the amount of time to run the script. Now a required
    step in the series. 

CHANGELOG:
    YYYY-MM-DD - <Author>: <Description of change>
    YYYY-MM-DD - <Author>: <Another change>

===============================================================================
"""

 ### PART 001: IMPORT USAGE TABLES, CREATE RES_IDS, JOIN TO SOD SF AND HOMESITE SOLD TABLES, EXPORT MONTHLY TABLES TO GDB
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
print("SCRIPT STARTED AT " + strftime("%m/%d/%Y %H:%M:%S", localtime()))

DATE = strftime("%d%b%y", localtime()).upper()
arcpy.ClearWorkspaceCache_management()
arcpy.env.overwriteOutput = True

MONTH_LIST = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']


### CHANGE CURRENT MONTH, USE 3 LETTER ABBREVIATION ###

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

for m in MONTH_LIST:
    MONTH = m
    YEAR = str(int(datetime.now().strftime("%y")))
    gdb_path = os.path.join(r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\01 MONTHLY RESULTS\GEODATABASE",
                            f"{CURRENT_MONTH}{YEAR}.gdb")
    out_table = os.path.join(gdb_path, f"COMMERCIAL_IRRIGATION_USAGE_{MONTH}{YEAR}")
    arcpy.ClearWorkspaceCache_management()  
    arcpy.conversion.ExcelToTable(r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\01 MONTHLY RESULTS\COMMERCIAL_TABLES" + "\\"+ CURRENT_MONTH + YEAR + "\\" + "COMMERCIAL_IRRIGATION_USAGE_" + MONTH + YEAR + ".xlsx",
                                    os.path.join(gdb_path, "COMMERCIAL_IRRIGATION_USAGE_" + MONTH + YEAR))
 