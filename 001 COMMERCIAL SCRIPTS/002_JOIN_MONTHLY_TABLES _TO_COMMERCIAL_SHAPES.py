"""
===============================================================================
SCRIPT NAME:    002_JOIN_MONTHLY_TABLES_TO_COMMERCIAL_SHAPES.py

AUTHOR:         Moises Herrera
CREATED ON:     2025-11-20 (YYYY-MM-DD)
LAST UPDATED:   <Last Update Date> (YYYY-MM-DD)
VERSION:        1.0.0

DESCRIPTION:
    Joins the output tables of 001 and 001_5 to in the commercial shapes found in the Commercial Irrigation Data folder

USAGE:
    Hit run, run in debug mode

REQUIREMENTS:
    - Python version: 3.11.8
    - Dependencies: arcpy, datetime, time

INPUTS:
    <The GDB that stores the tables from previous 001 and 001_5 outputs>
    <A GDB workspace path, whose GDB stores the commercial irrrigation shapes>

OUTPUTS:
    ESRI GDB Geometry polygon feature classes - one for each month

NOTES:
    Part two of a four part series. Must change current month. 

CHANGELOG:
    YYYY-MM-DD - <Author>: <Description of change>
    YYYY-MM-DD - <Author>: <Another change>

===============================================================================
"""

### PART 002: LOOP THROUGH LIST OF TABLES CREATED IN STEP 001, JOIN EACH TABLE TO COMMERCIAL SHAPES, COPY JOINED FEATURES TO NEW SHAPE OUTPUT
import arcpy
from datetime import datetime, timedelta
import time
from time import localtime, strftime
DATE = strftime("%d%b%y", localtime()).upper()
CURRENT_MONTH = (datetime.now() - timedelta(days=30)).strftime("%b").upper() 
YEAR = str(int(datetime.now().strftime("%y")))

### JOIN TO COMMERCIAL SHAPES FOR EACH MONTH ###
### EXPORT COMMERCIAL SHAPES TO IRRIGATION GDB
GDB = r"A:\TEST_LOCATION\03 PROGRAMMING\0008_IRRIGATION_USAGE\OUTPUT_GDB" + "\\" + CURRENT_MONTH + YEAR  + ".gdb"
arcpy.env.workspace = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\05 DELIVERABLES\00 GIS\IRRIGATION_USAGE.gdb"
arcpy.env.overwriteOutput = True
arcpy.conversion.ExportFeatures('IRRIGATION_CIAC', GDB + "\\" + "IRRIGATION_CIAC" + DATE)

### SWITCH WORKSPACE TO IRR GDB, SET COMMERCIAL SHAPES AS INPUT FOR FEATURE LAYER JOIN
arcpy.env.workspace = GDB
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

inFeatures = GDB + "\\" + "IRRIGATION_CIAC" + DATE

### LOOP THRU LIST OF TABLES IN IRR GDB, MAKE THEM INTO TABLE VIEWS, JOIN TO FEATURE LAYER OF COMMERCIAL SHAPES, AND EXPORT FOR EACH MONTH
tables = arcpy.ListTables()
for table in tables:
    if "COMMERCIAL_IRRIGATION_USAGE" in table:
        empty_fields = ["Meter_No", "Billing_Start", "Billing_End", "Billing_Days", "County", "Date"]
        arcpy.management.DeleteField(table, empty_fields)
        print(table)

        commercial_shapes = arcpy.MakeFeatureLayer_management(in_features=inFeatures, out_layer='COMMERCIAL_SHAPES_FOR_JOIN')
        fc_fields = arcpy.ListFields(commercial_shapes)
        for f in fc_fields:
            print(f"{f.name} --> {f.domain}")

        table_view = arcpy.MakeTableView_management(in_table=table, out_view='TABLE_VIEW')
        t_fields = arcpy.ListFields(table_view)
        for f in t_fields:
            print(f"{f.name} --> {f.domain}")
        print("JOINING COMMERCIAL SHAPES AND " + table)
        arcpy.management.AddJoin(in_layer_or_view='COMMERCIAL_SHAPES_FOR_JOIN', in_field='ACC_PREFIX', join_table='TABLE_VIEW', join_field='Account_Prefix', join_type = "KEEP_COMMON")
        print("EXPORTING JOIN OF COMMERCIAL SHAPES AND " + table + "\n")
        arcpy.CopyFeatures_management(in_features='COMMERCIAL_SHAPES_FOR_JOIN', out_feature_class= GDB + "\\" + 'COMM_IRRIGATION_USAGE_' + table.split("_")[3])
        #print(script completed)