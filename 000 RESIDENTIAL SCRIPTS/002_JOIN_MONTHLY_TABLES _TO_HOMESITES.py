"""
===============================================================================
SCRIPT NAME:    002_JOIN_MONTHLY_TABLES_TO_HOMESITES.py

AUTHOR:         Nathan Shull
CREATED ON:     2023-11-01 (YYYY-MM-DD)
LAST UPDATED:   <Last Update Date> (YYYY-MM-DD)
VERSION:        1.0.0

DESCRIPTION:
    Joins monthly tables in the geodatabase to the core homesites

USAGE:
    Hit run, run in debug mode

REQUIREMENTS:
    - Python version: 3.11.8
    - Dependencies: arcpy, datetime, time

INPUTS:
    <Description of input files, parameters, databases, or layers used.>

OUTPUTS:
    Core Homesites, GDB Tables from 001_5

NOTES:
    Part two of a four part series. Must change current month. 

CHANGELOG:
    YYYY-MM-DD - <Author>: <Description of change>
    YYYY-MM-DD - <Author>: <Another change>

===============================================================================
"""

### PART 002: LOOP THROUGH LIST OF TABLES CREATED IN STEP 001, JOIN EACH TABLE TO HOMESITES, COPY JOINED FEATURES TO NEW SHAPE OUTPUT
import arcpy
import datetime
import time
from time import localtime, strftime
DATE = strftime("%d%b%y", localtime()).upper()
CURRENT_MONTH = "FEB" ####################################### REQUIRES USER INPUT TO CHANGE, USE 3-LETTER MONTH KEY FOR CURRENT MONTH YOU ARE RUNNING
YEAR = "26" ###################### MAKE SURE IT IS CURRENT YEAR

### JOIN TO HOMESITES FOR EACH MONTH ###
### EXPORT HOMESITES TO IRRIGATION GDB
GDB = r"A:\TEST_LOCATION\03 PROGRAMMING\0008_IRRIGATION_USAGE\OUTPUT_GDB" + "\\" + CURRENT_MONTH + YEAR  + ".gdb"
arcpy.env.workspace = r"A:\GIS\00 DATA\02 GEODATABASES\800 SCRATCH_DATA\MH_SCRATCH_DATA\HERRERA_SCRATCH_DATA.gdb"
arcpy.env.overwriteOutput = True
arcpy.conversion.ExportFeatures('HOMESITE', GDB + "\\" + "HOMESITES_" + DATE)

### SWITCH WORKSPACE TO IRR GDB, SET HOMESITES AS INPUT FOR FEATURE LAYER JOIN
arcpy.env.workspace = GDB
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

inFeatures = GDB + "\\" + "HOMESITES_" + DATE

### LOOP THRU LIST OF TABLES IN IRR GDB, MAKE THEM INTO TABLE VIEWS, JOIN TO FEATURE LAYER OF HOMESITES, AND EXPORT FOR EACH MONTH
tables = arcpy.ListTables()
for table in tables:
    if "IRRIGATION_USAGE" in table:
        print(table)
        arcpy.MakeFeatureLayer_management(in_features=inFeatures, out_layer='HOMESITES_FOR_JOIN')
        arcpy.MakeTableView_management(in_table=table, out_view='TABLE_VIEW')
        print("JOINING HOMESITES AND " + table)
        arcpy.AddJoin_management(in_layer_or_view='HOMESITES_FOR_JOIN', in_field='RES_ID', join_table='TABLE_VIEW', join_field='RES_ID', join_type = "KEEP_COMMON")
        print("EXPORTING JOIN OF HOMESITES AND " + table + "\n")
        arcpy.CopyFeatures_management(in_features='HOMESITES_FOR_JOIN', out_feature_class= GDB + "\\" + 'HOMESITES_IRRIGATION_USAGE_' + table.split("_")[2])
        #print(script completed)