"""
===============================================================================
SCRIPT NAME:    INITIALIE_SCRIPTS.py

AUTHOR:         Moises Herrera
CREATED ON:     2026-04-01 (YYYY-MM-DD)
LAST UPDATED:   2026-04-08 (YYYY-MM-DD)
VERSION:        1.2.0 (e.g., 1.0.0)

DESCRIPTION:
    Support function, run this before 001_PREP_MONTHLY_USAGE_DATA.py

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


    Contains various support functions used to prepare irrigation usage final tables before uploading to ArcGIS Enterprise Dashboard.
    *To begin Irrigation Data preparation process just ensure that new data exists in bin, a new folder for the current running month 
    is in 00SUPPORT/TABLES_RECVD/RESIDENTIAL, and the MSI100 and MSI102 are in the new folder for the curreny running month.*

    Next Automation Implementation: automate MOD creation and transfer from bin, checking and converting too correct excel type: workbook

CHANGELOG:
    2026-04-08 - Moises Herrera: Making some updates, documentation, adding Copy Features.
    YYYY-MM-DD - <Author>: <Another change>
    YYYY-MM-DD - <Author>: <Another change>
===============================================================================
"""
# Export out the most up to date homesite data from Dev_Residential to A:\GIS\00 DATA\02 GEODATABASES\800 SCRATCH_DATA\MH_SCRATCH_DATA\HERRERA_SCRATCH_DATA.gdb\HOMESITE


import shutil

import pandas as pd
import numpy as np
import arcpy
from sqlalchemy import DATE
from datetime import datetime, timedelta
import time
import os
import win32com.client as win32


#DECLARE GLOBAL VARIABLES
CURRENT_MONTH = (datetime.now() - timedelta(days=30)).strftime("%b").upper()
DATE = time.strftime("%d%b%y", time.localtime()).upper()
YEAR = str(int(datetime.now().strftime("%y")))
reference_date = datetime.now() - timedelta(days=30)

month_num = reference_date.month

target_sheets = [
    f"{month_num}-{reference_date.strftime('%Y')}",
    f"{month_num}-{reference_date.strftime('%y')}"
]

print(target_sheets)

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

### CONVERT ALL IN BIN TO MICROSOFT EXCEL WORKSHEETS
directory = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\00 SUPPORT\TABLES_RECVD\BIN\Residential"
excel = win32.gencache.EnsureDispatch('Excel.Application')
excel.Visible = False

try:
    for filename in os.listdir(directory):
        if filename.endswith(".xls") and not filename.endswith(".xlsx"):
            xls_path = os.path.join(directory, filename)
            xlsx_filename = os.path.splitext(filename)[0] + ".xlsx"
            xlsx_path = os.path.join(directory, xlsx_filename)
            workbook = excel.Workbooks.Open(xls_path)

            workbook.SaveAs(xlsx_path, FileFormat=51)

            workbook.Close(False)
            print(f"Converted: {filename} - > {xlsx_filename}")

finally:
    excel.Quit()

# -----------------------------------
# STEP 2: Modify all .xlsx files
# -----------------------------------

for filename in os.listdir(directory):

    if filename.lower().endswith(".xlsx"):

        xlsx_path = os.path.join(directory, filename)

        # Get workbook sheet names
        workbook = pd.ExcelFile(xlsx_path)

        matching_sheet = None

        for sheet in workbook.sheet_names:
            if sheet in target_sheets:
                matching_sheet = sheet
                break

        if not matching_sheet:
            print(f"Skipped: {filename}")
            continue

        # Read only target sheet
        df = pd.read_excel(
            xlsx_path,
            sheet_name=matching_sheet
        )

        fields_to_remove = [
            "Meter",
            "Irrigation Usage Fee",
            "County"
        ]

        df.drop(
            columns=fields_to_remove,
            inplace=True,
            errors="ignore"
        )

        df.rename(
            columns={
                "Irrigation Usage": "Usage"
            },
            inplace=True
        )

        # Replace only that sheet
        with pd.ExcelWriter(
            xlsx_path,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:

            df.to_excel(
                writer,
                sheet_name=matching_sheet,
                index=False
            )

        print(f"Updated: {filename} [{matching_sheet}]")


final_directory = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\00 SUPPORT\TABLES_RECVD\RESIDENTIAL" + "\\" + CURRENT_MONTH + YEAR + "\\" + "MOD"

# Create destination if it doesn't exist
os.makedirs(final_directory, exist_ok=True)

for filename in os.listdir(directory):
    
    source_path = os.path.join(directory, filename)

    # Skip directories
    if not os.path.isfile(source_path):
        continue

    destination_path = os.path.join(final_directory, filename)

    shutil.move(source_path, destination_path)

    print(f"Moved: {filename}")