"""
===============================================================================
SCRIPT NAME:    004_FINAL_JOINS_TO_COMMERCIAL_SHAPES.py

AUTHOR:         Moises Herrera
CREATED ON:     2025-11-24 (YYYY-MM-DD)
LAST UPDATED:   0000-00-00 (YYYY-MM-DD)
VERSION:        1.0.0

DESCRIPTION:
    Adds projection data to commercial shapes by merging with gdb table from 003.

USAGE:
    Hit run, run from debug mode

REQUIREMENTS:
    - Python version: 3.11.8
    - Dependencies: arcpy, pandas, datetime, time

INPUTS:
    Curent month commercial irrigation GDB, core commercial shapes

OUTPUTS:
    Final commercial shapes to GDB

NOTES:
    Fourth part of four part series. Export new commercial shapes if any. Change month to current month.

CHANGELOG:
    0000-00-00 - FIRSTNAME LASTNAME:

===============================================================================
"""

### PART 004: JOIN MERGED DATAFRAME WITH ALL MONTHS POPULATED TO COMMERCIAL SHAPES AS _FINAL

### JOIN FINAL TABLE TO COMMERCIAL SHAPES ###
import os
import arcpy
import pandas as pd
import datetime
import re
import time
import calendar
from collections import deque
import math
from time import localtime, strftime

from datetime import datetime, timedelta

CURRENT_MONTH = (datetime.now() - timedelta(days=30)).strftime("%b").upper()
YEAR = str(int(datetime.now().strftime("%y")))

arcpy.ClearWorkspaceCache_management()

DATE = strftime("%d%b%y", localtime()).upper()
GDB =  r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\01 MONTHLY RESULTS\GEODATABASE" + "\\" + CURRENT_MONTH + YEAR  + ".gdb"
arcpy.env.workspace = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\00 GEODATABASE\IRRIGATION_USAGE.gdb"
arcpy.env.overwriteOutput = True

### FEATURE ALREADY EXISTS FROM PART 002 ###
arcpy.conversion.ExportFeatures('IRRIGATION_CIAC', GDB + "\\" + "IRRIGATION_CIAC" + DATE)


### SWITCH WORKSPACE TO IRR GDB, SET COMMERCIAL_SHAPES AS INPUT FOR FEATURE LAYER JOIN
arcpy.env.workspace = GDB
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

inFeatures = GDB + "\\" + "IRRIGATION_CIAC" + DATE


### LOOP THRU LIST OF TABLES IN IRR GDB, MAKE THEM INTO TABLE VIEWS, JOIN TO COMMERCIAL_SHAPES, AND EXPORT FOR EACH MONTH
tables = arcpy.ListTables()
for table in tables:
    if "COMMERCIAL_IRRIGATION_USAGE_" + CURRENT_MONTH + YEAR + "_FINAL" in table:
        arcpy.MakeFeatureLayer_management(in_features=inFeatures, out_layer='COMMERCIAL_SHAPES_FOR_JOIN')
        arcpy.MakeTableView_management(in_table=table, out_view='TABLE_VIEW')
        arcpy.AddJoin_management(in_layer_or_view='COMMERCIAL_SHAPES_FOR_JOIN', in_field='ACC_PREFIX', join_table='TABLE_VIEW', join_field='Account_Prefix', join_type = "KEEP_COMMON")
        arcpy.CopyFeatures_management(in_features='COMMERCIAL_SHAPES_FOR_JOIN', out_feature_class= GDB + "\\" + 'COMMERCIAL_SHAPES_IRRIGATION_USAGE_' + CURRENT_MONTH + YEAR + "_FINAL")

#
fc_name = 'COMMERCIAL_SHAPES_IRRIGATION_USAGE_' + CURRENT_MONTH + YEAR + "_FINAL"
fc = os.path.join(GDB, fc_name)

### DELETE UNNECESSARY FIELDS ###
arcpy.management.DeleteField(
    in_table=fc,
    drop_field=["GIS_SQFT", "SQFT_DIFF", "Type_1", "PROVIDER_1"],
    method="DELETE_FIELDS"
)

arcpy.management.AlterField(
    in_table=fc,
    field="Customer_Name",
    new_field_name="Property_Name",
    new_field_alias="Property_Name"
)

arcpy.management.AlterField(
    in_table=fc,
    field="Customer_Address",
    new_field_name="Property_Address",
    new_field_alias="Property_Address"
)

arcpy.ClearWorkspaceCache_management()
time.sleep(5)

### REMOVE DUPLICATES BASED ON Account_Prefix ###
arcpy.management.DeleteIdentical(
    in_dataset=fc,
    fields=["Account_Prefix"]
)

#ADD CUR_USAGE nad CUR_PCT FIELDS
CURRENT_MONTH_USAGE = CURRENT_MONTH + "_USAGE"
CURRENT_MONTH_PCT = CURRENT_MONTH + "_PCT"

arcpy.management.AddField(fc, "CUR_USAGE", "LONG")
arcpy.management.AddField(fc, "CUR_PCT", "LONG")
arcpy.management.CalculateField(fc, "CUR_USAGE", f"!{CURRENT_MONTH_USAGE}!", "PYTHON3")
arcpy.management.CalculateField(fc, "CUR_PCT", f"!{CURRENT_MONTH_PCT}!", "PYTHON3")


### EXPORT FINAL FEATURES TO DELIVERABLE GDB ###
out_fc = os.path.join(
    r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\00 GEODATABASE\IRRIGATION_USAGE.gdb",
    fc_name
)

arcpy.CopyFeatures_management(
    in_features=fc,
    out_feature_class=out_fc
)