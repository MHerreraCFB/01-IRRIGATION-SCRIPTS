"""
===============================================================================
SCRIPT NAME:    INITIALIE_SCRIPTS.py

AUTHOR:         Moises Herrera
CREATED ON:     2026-04-01 (YYYY-MM-DD)
LAST UPDATED:   2026-04-08 (YYYY-MM-DD)
VERSION:        1.2.0 (e.g., 1.0.0)

DESCRIPTION:
    Support function to be used in 001_PREP_MONTHLY_USAGE_DATA.py

USAGE:
    Hit run or run in debug mode

REQUIREMENTS:
    - Python version: 3.11.8
    - Dependencies: arcpy, os, pandas, numpy, openpyxl, time, sqlalchemy

INPUTS:
    Varies on function used

OUTPUTS:
    Varies on function used

NOTES:
    Contains various support functions used to prepare irrigation usage final tables before uploading to ArcGIS Enterprise Dashboard

CHANGELOG:
    2026-04-08 - Moises Herrera: Making some updates, documentation, adding Copy Features.
    YYYY-MM-DD - <Author>: <Another change>
    YYYY-MM-DD - <Author>: <Another change>
===============================================================================
"""
# Export out the most up to date homesite data from Dev_Residential to A:\GIS\00 DATA\02 GEODATABASES\800 SCRATCH_DATA\MH_SCRATCH_DATA\HERRERA_SCRATCH_DATA.gdb\HOMESITE

from datetime import datetime, timedelta
import time

import pandas as pd
import numpy as np
import arcpy
from sqlalchemy import DATE

#DECLARE GLOBAL VARIABLES

DATE = time.strftime("%d%b%y", time.localtime()).upper()
CURRENT_MONTH = (datetime.now() - timedelta(days=30)).strftime("%b").upper()
YEAR = str(int(datetime.now().strftime("%y")))

def exportFeatures(inPath, outPath, xpression, name):
    success = False
    attempt = 0
    maxAttempts = 3
    while attempt < maxAttempts and not success:
        try:
            attempt += 1
            arcpy.conversion.ExportFeatures(inPath, outPath, xpression)
            success = True
        except Exception as e:
            if attempt < maxAttempts:
                print(f"Trying attempt {attempt + 1}....")
                               
    if not success:
        print("All attempts failed")



def copyFeatures(inPath, outPath, ):
    success = False
    attempt = 0
    maxAttempts = 3
    while attempt < maxAttempts and not success:
        try:
            attempt += 1
            arcpy.management.CopyFeatures(inPath, outPath)
            success = True
        except Exception as e:
            if attempt < maxAttempts:
                print(f"Trying attempt {attempt + 1}....")
                               
    if not success:
        print("All attempts failed")


def prepare_data():
    ### EXPORT HOMESITES TO IRRIGATION GDB
    GDB = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\00 GEODATABASE\IRRIGATION_USAGE.gdb"
    arcpy.env.workspace = r"A:\GIS\00 DATA\02 GEODATABASES\001 DEVELOPMENT\DEV_RESIDENTIAL.gdb"
    arcpy.env.overwriteOutput = True
    arcpy.management.CopyFeatures('RESIDENTIAL_FD\HOMESITE', GDB + "\\" + "HOMESITE")
    time.sleep(3)
    arcpy.management.CopyFeatures("RESIDENTIAL_FD" + "\\" + "NEIGHBORHOOD", GDB + "\\" + "NEIGHBORHOOD")
    arcpy.env.workspace = r"A:\GIS\00 DATA\02 GEODATABASES\001 DEVELOPMENT\DEV_ROADWAY.gdb"
    arcpy.env.overwriteOutput = True
    time.sleep(3)
    arcpy.management.CopyFeatures("ROADWAY_FD" + "\\" + "ROADWAY_EOP", GDB + "\\" + "ROADS_" + DATE)###
    

def prepare_closing_date_table():
    # APPEND MONTHLY MSI100 to COMPLETE TABLE AND WRITE BACK TO EXCEL WORKSHEET

    closing_date_table = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\00 SUPPORT\TABLES_RECVD\RESIDENTIAL" + "\\" + CURRENT_MONTH + YEAR + "\\" + "MSI100.xlsx"
    closing_date_complete_table = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\00 SUPPORT\TABLES_RECVD\RESIDENTIAL\CLOSING_DATE_COMPLETE\MSI100.xlsx"

    # Load data
    temp_cdt = pd.read_excel(closing_date_table)
    df_complete = pd.read_excel(closing_date_complete_table)

    # Append (concatenate)
    df_updated = pd.concat([df_complete, temp_cdt], ignore_index=True)

    # Write back to the original file
    df_updated.to_excel(closing_date_complete_table, index=False)


#INITIALIZATION STEPS
prepare_data()

### PREPARE CLOSING DATE TABLE ###
prepare_closing_date_table()