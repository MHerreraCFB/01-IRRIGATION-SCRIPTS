"""
===============================================================================
SCRIPT NAME:    004_FINAL_JOINS_TO_HOMESITES.py

AUTHOR:         Nathan Shull
CREATED ON:     2023-11-01 (YYYY-MM-DD)
LAST UPDATED:   2025-08-06 (YYYY-MM-DD)
VERSION:        1.0.0

DESCRIPTION:
    Adds projection data to homesites by merging with gdb table from 003. Merges tables with units and roads. 

USAGE:
    Hit run, run from debug mode

REQUIREMENTS:
    - Python version: 3.11.8
    - Dependencies: arcpy, pandas, datetime, time

INPUTS:
    Curent month irrigation GDB, core homesites, roads and units

OUTPUTS:
    Final homesites, final units, final roads to selecte GDB

NOTES:
    Fourth part of four part series. Export new neighborhoods/dev_residential and roadway_eop/dev_roadway to your scratch gdb. Change current month!
    PART 005: MERGE WITH MOST RECENT CONTRACTORS, USE RESULTS FROM IRRIGATION DATA SCRIPTS USE THE UNIT ENTRIES AND MERGE WITH THE FINAL UNITS OUTPUT SHOULD BE ONLY 4 or 5, FILL IN MISSING FIELDS LIKE UNIT, DISTRICT, UNIT JOIN AFTER MERGE.

CHANGELOG:
    2025-08-05 - Moises Herrera: Made all outputs into long integers rounded to a whole number. Output has no more decimals. Removed unneeded prefixes like SUM and MEAN from the final layer. All months are placed in reversed chronological order. 
    2025-08-06 - Moises Herrera: Added a UNIT_JOIN field to Roads and Units at the end of the script so that roads can be filtered by the unit that they are in spatially. 
    2025-09-11 - Moises Herrera: Making the road_join field for both homesites and roads the full road name with proper capitalization.

===============================================================================
"""

### PART 004: JOIN MERGED DATAFRAME WITH ALL MONTHS POPULATED TO HOMESITES, UNITS, AND ROADS, OUTPUT ALL AS _FINAL

### JOIN FINAL TABLE TO HOMESITES ###
import arcpy
import pandas as pd
import datetime
import re
import time
import calendar
from collections import deque
import math
from time import localtime, strftime
arcpy.ClearWorkspaceCache_management()

DATE = strftime("%d%b%y", localtime()).upper()
CURRENT_MONTH = (datetime.now() - datetime.timedelta(days=30)).strftime("%b").upper()
YEAR = str(int(datetime.now().strftime("%y")))
GDB =  r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\01 MONTHLY RESULTS\GEODATABASE" + "\\" + CURRENT_MONTH + YEAR  + ".gdb"

### MAKE WORKSPACE GDB, SET HOMESITES AS INPUT FOR FEATURE LAYER JOIN
arcpy.env.workspace = GDB
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

inFeatures = GDB + "\\" + "HOMESITES_" + DATE


### LOOP THRU LIST OF TABLES IN IRR GDB, MAKE THEM INTO TABLE VIEWS, JOIN TO HOMESITES, AND EXPORT FOR EACH MONTH
tables = arcpy.ListTables()
for table in tables:
    if "IRRIGATION_USAGE_" + CURRENT_MONTH + YEAR + "_FINAL" in table:
        arcpy.MakeFeatureLayer_management(in_features=inFeatures, out_layer='HOMESITES_FOR_JOIN')
        arcpy.MakeTableView_management(in_table=table, out_view='TABLE_VIEW')
        arcpy.AddJoin_management(in_layer_or_view='HOMESITES_FOR_JOIN', in_field='RES_ID', join_table='TABLE_VIEW', join_field='RES_ID', join_type = "KEEP_COMMON")
        arcpy.CopyFeatures_management(in_features='HOMESITES_FOR_JOIN', out_feature_class= GDB + "\\" + 'HOMESITES_IRRIGATION_USAGE_' + CURRENT_MONTH + YEAR + "_FINAL")

### ROADS NEXT ###
### ADD FIELD FOR ROADWAY JOIN, GET ROAD JOIN FIELD FROM ADDRESS USING UPDATE CURSOR ###
arcpy.management.AddField(GDB + "\\" + 'HOMESITES_IRRIGATION_USAGE_' + CURRENT_MONTH + YEAR + "_FINAL", "ROAD_JOIN", "TEXT")

fc = GDB + "\\" + 'HOMESITES_IRRIGATION_USAGE_' + CURRENT_MONTH + YEAR + "_FINAL"

keys = ["AVE", "CIR", "CT", "DR", "LN", "LOOP", "PATH", "PL", "RD", "RUN", "ST", "TER", "TRL", "WAY"]
vals = ["Avenue", "Circle", "Court", "Drive", "Lane", "Loop", "Path", "Place", "Road", "Run", "Street", "Terrace", "Trail", "Way"]
road_types_full = dict(zip(keys, vals))

# USE UPDATE CURSOR FOR ROAD_JOIN FIELD
fields = ['Address_1', 'ROAD_JOIN']
with arcpy.da.UpdateCursor(fc, fields) as cursor:
        for row in cursor:
            address = row[0]

            # Initialize variable
            road_type = None

            # Check if any road type key is in the address
            for key, type in road_types_full.items():
                if key in address:   # substring match
                    road_type = type
                    break 
            
            row[0] = row[0].strip()
            
            if road_type is not None:
                if len(row[0].split(" ")) > 3:
                    row[1] = row[0].split(" ")[1].title() + " " + row[0].split(" ")[2].title() + " " + road_type
                else:
                    row[1] = row[0].split(" ")[1].title() + " " + road_type
                cursor.updateRow(row)
            else:
                cursor.updateRow(row)

### DELETE UNNESSECARY FIELDS FROM HOMESITES ###
arcpy.management.DeleteField(GDB + "\\" + 'HOMESITES_IRRIGATION_USAGE_' + CURRENT_MONTH + YEAR + "_FINAL", ["TAB_CAT",
                                                                                                            "MAS_CAT",
                                                                                                            "LABEL_NAME",
                                                                                                           "DIST_PH",
                                                                                                           "GIS_ACRES",
                                                                                                           "RES_TYPE1",
                                                                                                           "RES_TYPE2",
                                                                                                           "RES_LABEL",
                                                                                                           "BOND_REQ",
                                                                                                            "STATUS",
                                                                                                            "EDIT_DATE",
                                                                                                            "DATA_SOURCE",
                                                                                                            "ORACLE",
                                                                                                            "EX_1",
                                                                                                            "EX_2",
                                                                                                            "EX_3",
                                                                                                            "EX_4",
                                                                                                            "EX_5",
                                                                                                            "EX_6",
                                                                                                            "EX_7",
                                                                                                            "EX_8",
                                                                                                            "EX_9",
                                                                                                            "POSTAL",
                                                                                                            "NRC",
                                                                                                            "VRC",
                                                                                                            "RRC",
                                                                                                            "WATER_SEWER",
                                                                                                            "IRRIGATION",
                                                                                                            "ELECTRIC",
                                                                                                            "GAS",
                                                                                                            "VILLAGE",
                                                                                                            "STR",
                                                                                                            "LAT",
                                                                                                            "LONG",
                                                                                                            "LINK",
                                                                                                            "LINK_DISPLAY",
                                                                                                            "RES_ID_CDT",
                                                                                                           "OBJECTID_1",
                                                                                                           "RES_ID_1",
                                                                                                           "RES_TYPE7",
                                                                                                           "LETTERS_SENT",
                                                                                                           "DATE_SENT",
                                                                                                           "COW_ACCEPTANCE",
                                                                                                           "RES_TYPE3",
                                                                                                           "TAB_ID",
                                                                                                           "GOLF_VIEW",
                                                                                                           "WETLAND_PRESERVE_VIEW",
                                                                                                           "LAKE_POND_VIEW",
                                                                                                           "TWO_PLUS_VIEW",
                                                                                                           "LRP_ID",
                                                                                                           "FRNT_UTL",
                                                                                                           "UNIT_ID",
                                                                                                           "REGION",
                                                                                                           "VIEW",
                                                                                                           "LOCATION",
                                                                                                           "BI_VIEW"], "DELETE_FIELDS")


def get_last_12_months(current_month_abbr):
    """
    Returns a list of MONTHS going 12 months back from current_month_abbr
    in reverse chronological order.
    """
    current_month_abbr = current_month_abbr.upper()
    months_abbr = [month.upper() for month in calendar.month_abbr[1:]]  # ['JAN', ..., 'DEC']
    month_abbr_to_num = {abbr: idx for idx, abbr in enumerate(months_abbr)}  # JAN=0, ..., DEC=11

    if current_month_abbr not in month_abbr_to_num:
        raise ValueError(f"Invalid month abbreviation: {current_month_abbr}")

    current_index = month_abbr_to_num[current_month_abbr]

    # Collect 12 months in reverse order
    months = []
    for i in range(12):
        index = (current_index - i) % 12
        month = months_abbr[index]
        months.append(month)

    return months


def generate_dissolve_stats_fields():
    months = get_last_12_months(CURRENT_MONTH)
    stats = []
    for m in months:
        stats.append([f"{m}_USAGE", "SUM"])
        stats.append([f"{m}_RMD", "SUM"])
        stats.append([f"{m}_PCT", "MEAN"])
    # Add your summary fields if needed:
    stats += [["USAGE_4MO", "SUM"],
              ["RMD_4MO", "SUM"],
              ["PCT_4MO", "MEAN"]]
    return stats

stats_fields = generate_dissolve_stats_fields()

 ### DISSOLVE ON UNIT, DISTRICT AND THEN ON ROAD_JOIN, DISTRICT ###
# Then run dissolve with stats_fields:
arcpy.management.Dissolve(
    in_features=GDB + "\\" + 'HOMESITES_IRRIGATION_USAGE_' + CURRENT_MONTH + YEAR + "_FINAL",
    out_feature_class=GDB + "\\" + 'UNITS_FOR_JOIN_' + CURRENT_MONTH + YEAR,
    dissolve_field=["RES_TYPE5", "DISTRICT"],
    statistics_fields=stats_fields
)

arcpy.management.Dissolve(
    in_features=GDB + "\\" + 'HOMESITES_IRRIGATION_USAGE_' + CURRENT_MONTH + YEAR + "_FINAL",
    out_feature_class=GDB + "\\" + 'ROADS_FOR_JOIN_' + CURRENT_MONTH + YEAR,
    dissolve_field=["ROAD_JOIN", "DISTRICT"],
    statistics_fields=stats_fields
)

def convert_fields_to_long_no_prefix(feature_class, method="round"):
    if method not in ["round", "floor", "ceil"]:
        raise ValueError("Method must be 'round', 'floor', or 'ceil'")

    arcpy.env.overwriteOutput = True
    fields = arcpy.ListFields(feature_class)

    for field in fields:
        original_name = field.name

        # Skip non-numeric, geometry, or identifier fields
        if (
            field.type not in ["Double", "Single", "Float"]
            or original_name.startswith("OID")
            or original_name in ["Shape_Length", "Shape_Area"]
        ):
            continue

        # Strip prefixes if they exist
        stripped_name = re.sub(r"^(SUM_|MEAN_)", "", original_name)

        # Avoid name conflicts by adding "_long" temporarily
        temp_field = stripped_name + "_long"

        # Add LONG field
        arcpy.AddField_management(feature_class, temp_field, "LONG")

        # Copy converted values
        with arcpy.da.UpdateCursor(feature_class, [original_name, temp_field]) as cursor:
            for row in cursor:
                val = row[0]
                if val is None:
                    row[1] = None
                else:
                    if method == "round":
                        row[1] = int(round(val))
                    elif method == "floor":
                        row[1] = int(math.floor(val))
                    elif method == "ceil":
                        row[1] = int(math.ceil(val))
                cursor.updateRow(row)

        # Delete original and rename long field
        arcpy.DeleteField_management(feature_class, original_name)
        arcpy.AlterField_management(
            feature_class,
            temp_field,
            new_field_name=stripped_name,
            new_field_alias=stripped_name
        )

    print(f"All eligible numeric fields in {feature_class} were converted to LONG using method '{method}'.")



dissolved_units = GDB + "\\" + 'UNITS_FOR_JOIN_' + CURRENT_MONTH + YEAR
dissolved_roads = GDB + "\\" + 'ROADS_FOR_JOIN_' + CURRENT_MONTH + YEAR

convert_fields_to_long_no_prefix(dissolved_units, method="round")
convert_fields_to_long_no_prefix(dissolved_roads, method="round")

arcpy.ClearWorkspaceCache_management()
time.sleep(5)
### DISSOLVE UNITS ON RES_TYPE5 AND DISTRICT ###
arcpy.management.Dissolve(r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\00 GEODATABASE\IRRIGATION_USAGE.gdb\NEIGHBORHOOD", GDB + "\\" + 'UNITS_SHAPE_FOR_JOIN_' + CURRENT_MONTH + YEAR, 
                          ["RES_TYPE5", "DISTRICT"])

### EXPORT D14, NOT D14 FOR BOTH UNITS, ROADS
arcpy.conversion.ExportFeatures(GDB + "\\" + 'UNITS_FOR_JOIN_' + CURRENT_MONTH + YEAR, GDB + "\\" + 'UNITS_TABLE_FOR_JOIN_D14_' + CURRENT_MONTH + YEAR, "DISTRICT LIKE '14'")
arcpy.conversion.ExportFeatures(GDB + "\\" + 'UNITS_FOR_JOIN_' + CURRENT_MONTH + YEAR, GDB + "\\" + 'UNITS_TABLE_FOR_JOIN_MCDDA_' + CURRENT_MONTH + YEAR, "DISTRICT LIKE 'MCDDA'")
arcpy.conversion.ExportFeatures(GDB + "\\" + 'UNITS_FOR_JOIN_' + CURRENT_MONTH + YEAR, GDB + "\\" + 'UNITS_TABLE_FOR_JOIN_NOT_D14_MCDDA_' + CURRENT_MONTH + YEAR,"DISTRICT NOT LIKE '14' AND DISTRICT NOT LIKE 'MCDDA'")
arcpy.conversion.ExportFeatures(GDB + "\\" + 'ROADS_FOR_JOIN_' + CURRENT_MONTH + YEAR, GDB + "\\" + 'ROADS_TABLE_FOR_JOIN_D14_' + CURRENT_MONTH + YEAR, "DISTRICT LIKE '14'")
arcpy.conversion.ExportFeatures(GDB + "\\" + 'ROADS_FOR_JOIN_' + CURRENT_MONTH + YEAR, GDB + "\\" + 'ROADS_TABLE_FOR_JOIN_MCDDA_' + CURRENT_MONTH + YEAR, "DISTRICT LIKE 'MCDDA'")
time.sleep(10)
arcpy.conversion.ExportFeatures(GDB + "\\" + 'ROADS_FOR_JOIN_' + CURRENT_MONTH + YEAR, GDB + "\\" + 'ROADS_TABLE_FOR_JOIN_NOT_D14_MCDDA_' + CURRENT_MONTH + YEAR,"DISTRICT NOT LIKE '14' AND DISTRICT NOT LIKE 'MCDDA'")

### JOIN DISSOLVE OUTPUT TO ROADS, UNITS (DIST 14, MCDDA, NOT DIST 14/MCDDA) AND MERGE OUTPUTS FOR ROADS, UNITS ###
### EXPORT HOMESITES TO IRRIGATION GDB
arcpy.env.workspace = GDB
arcpy.env.overwriteOutput = True
arcpy.conversion.ExportFeatures('UNITS_SHAPE_FOR_JOIN_' + CURRENT_MONTH + YEAR, GDB + "\\" + "UNITS_FOR_JOIN_D14_" + DATE, "DISTRICT LIKE '14'")
arcpy.conversion.ExportFeatures('UNITS_SHAPE_FOR_JOIN_' + CURRENT_MONTH + YEAR, GDB + "\\" + "UNITS_FOR_JOIN_MCDDA_" + DATE, "DISTRICT LIKE 'MCDDA'")
time.sleep(10)
arcpy.conversion.ExportFeatures('UNITS_SHAPE_FOR_JOIN_' + CURRENT_MONTH + YEAR, GDB + "\\" + "UNITS_FOR_JOIN_NOT_D14_MCDDA_" + DATE, "DISTRICT NOT LIKE '14' AND DISTRICT NOT LIKE 'MCDDA'")

arcpy.env.workspace = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\00 GEODATABASE\IRRIGATION_USAGE.gdb"
arcpy.env.overwriteOutput = True
arcpy.management.CopyFeatures("ROADS_" + DATE, GDB + "\\" + "ROADS_" + DATE)

arcpy.env.workspace = GDB
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False

### ADD ROAD_JOIN FIELD TO ROADWAYS
arcpy.management.AddField(GDB + "\\" + "ROADS_" + DATE, "ROAD_JOIN", "TEXT")

fc = GDB + "\\" + "ROADS_" + DATE
fields = ['LABEL_NAME', 'ROAD_JOIN']

### REMOVE WHITESPACE FROM ROADS ###
with arcpy.da.UpdateCursor(fc, fields) as cursor:
    for row in cursor:
        if ((row[0] is not None) and (row[0] != '')):
            row[0] = row[0].strip()
        cursor.updateRow(row)

# USE UPDATE CURSOR FOR ROAD_JOIN FIELD
expression = "'LABEL_NAME' IS NOT NULL AND 'LABEL_NAME' <> ''"
with arcpy.da.UpdateCursor(fc, fields, expression) as cursor:
    for row in cursor:
        if ((row[0] is not None) and (row[0] != '')):

            try:
                if len(row[0].split(" ")) > 2:
                    row[1] = row[0].split(" ")[0] + " " + row[0].split(" ")[1] + " " + row[0].split(" ")[2]
                else:
                    row[1] = row[0].split(" ")[0] + " " + row[0].split(" ")[1]
            except: pass
            cursor.updateRow(row)
            
arcpy.conversion.ExportFeatures(GDB + "\\" + "ROADS_" + DATE, GDB + "\\" + "ROADS_FOR_JOIN_D14_" + DATE, "DISTRICT LIKE '14'")
arcpy.conversion.ExportFeatures(GDB + "\\" + "ROADS_" + DATE, GDB + "\\" + "ROADS_FOR_JOIN_MCDDA_" + DATE, "DISTRICT LIKE 'MCDDA'")
time.sleep(10)
arcpy.conversion.ExportFeatures(GDB + "\\" + "ROADS_" + DATE, GDB + "\\" + "ROADS_FOR_JOIN_NOT_D14_MCDDA_" + DATE, "DISTRICT NOT LIKE '14' AND DISTRICT NOT LIKE 'MCDDA'")

### SWITCH WORKSPACE TO IRR GDB
arcpy.env.workspace = GDB
arcpy.env.overwriteOutput = True
arcpy.env.qualifiedFieldNames = False


### JOIN TO UNITS, ONE BY D14, ONE FOR OTHER DISTRICTS, MERGE OUPUTS
### UNITS D14
inFeatures = GDB + "\\" + "UNITS_FOR_JOIN_D14_" + DATE
arcpy.MakeFeatureLayer_management(in_features=inFeatures, out_layer='UNITS_FOR_JOIN1')
arcpy.MakeTableView_management(in_table='UNITS_TABLE_FOR_JOIN_D14_' + CURRENT_MONTH + YEAR, out_view='TABLE_VIEW1')
arcpy.AddJoin_management(in_layer_or_view='UNITS_FOR_JOIN1', in_field='RES_TYPE5', join_table='TABLE_VIEW1', join_field='RES_TYPE5', join_type = "KEEP_COMMON")
arcpy.CopyFeatures_management(in_features='UNITS_FOR_JOIN1', out_feature_class= GDB + "\\" + 'UNITS_JOIN_D14_' + DATE)
### UNITS MCDDA
inFeatures = GDB + "\\" + "UNITS_FOR_JOIN_MCDDA_" + DATE
arcpy.MakeFeatureLayer_management(in_features=inFeatures, out_layer='UNITS_FOR_JOIN2')
arcpy.MakeTableView_management(in_table='UNITS_TABLE_FOR_JOIN_MCDDA_' + CURRENT_MONTH + YEAR, out_view='TABLE_VIEW2')
arcpy.AddJoin_management(in_layer_or_view='UNITS_FOR_JOIN2', in_field='RES_TYPE5', join_table='TABLE_VIEW2', join_field='RES_TYPE5', join_type = "KEEP_COMMON")
arcpy.CopyFeatures_management(in_features='UNITS_FOR_JOIN2', out_feature_class= GDB + "\\" + 'UNITS_JOIN_MCDDA_' + DATE)
### REST OF UNITS
inFeatures = GDB + "\\" + "UNITS_FOR_JOIN_NOT_D14_MCDDA_" + DATE
arcpy.MakeFeatureLayer_management(in_features=inFeatures, out_layer='UNITS_FOR_JOIN3')
arcpy.MakeTableView_management(in_table='UNITS_TABLE_FOR_JOIN_NOT_D14_MCDDA_' + CURRENT_MONTH + YEAR, out_view='TABLE_VIEW3')
arcpy.AddJoin_management(in_layer_or_view='UNITS_FOR_JOIN3', in_field='RES_TYPE5', join_table='TABLE_VIEW3', join_field='RES_TYPE5', join_type = "KEEP_COMMON")
arcpy.CopyFeatures_management(in_features='UNITS_FOR_JOIN3', out_feature_class= GDB + "\\" + 'UNITS_JOIN_NOT_D14_MCDDA_' + DATE)
### MERGE THE 3
arcpy.management.Merge([GDB + "\\" + 'UNITS_JOIN_D14_' + DATE, GDB + "\\" + 'UNITS_JOIN_MCDDA_' + DATE, GDB + "\\" + 'UNITS_JOIN_NOT_D14_MCDDA_' + DATE], GDB + "\\" + "UNITS_IRRIGATION_USAGE_" + CURRENT_MONTH + YEAR + "_FINAL")


### JOIN TO ROADS, ONE BY D14, ONE FOR OTHER DISTRICTS, MERGE OUPUTS
### UNITS D14
inFeatures = GDB + "\\" + "ROADS_FOR_JOIN_D14_" + DATE
arcpy.MakeFeatureLayer_management(in_features=inFeatures, out_layer='ROADS_FOR_JOIN1')
arcpy.MakeTableView_management(in_table='ROADS_TABLE_FOR_JOIN_D14_' + CURRENT_MONTH + YEAR, out_view='TABLE_VIEW3')
arcpy.AddJoin_management(in_layer_or_view='ROADS_FOR_JOIN1', in_field='ROAD_JOIN', join_table='TABLE_VIEW3', join_field='ROAD_JOIN', join_type = "KEEP_COMMON")
arcpy.CopyFeatures_management(in_features='ROADS_FOR_JOIN1', out_feature_class= GDB + "\\" + 'ROADS_JOIN_D14_' + DATE)
### UNITS D14
inFeatures = GDB + "\\" + "ROADS_FOR_JOIN_MCDDA_" + DATE
arcpy.MakeFeatureLayer_management(in_features=inFeatures, out_layer='ROADS_FOR_JOIN3')
arcpy.MakeTableView_management(in_table='ROADS_TABLE_FOR_JOIN_MCDDA_' + CURRENT_MONTH + YEAR, out_view='TABLE_VIEW5')
arcpy.AddJoin_management(in_layer_or_view='ROADS_FOR_JOIN3', in_field='ROAD_JOIN', join_table='TABLE_VIEW5', join_field='ROAD_JOIN', join_type = "KEEP_COMMON")
arcpy.CopyFeatures_management(in_features='ROADS_FOR_JOIN3', out_feature_class= GDB + "\\" + 'ROADS_JOIN_MCDDA_' + DATE)
### REST OF UNITS
inFeatures = GDB + "\\" + "ROADS_FOR_JOIN_NOT_D14_MCDDA_" + DATE
arcpy.MakeFeatureLayer_management(in_features=inFeatures, out_layer='ROADS_FOR_JOIN2')
arcpy.MakeTableView_management(in_table='ROADS_TABLE_FOR_JOIN_NOT_D14_MCDDA_' + CURRENT_MONTH + YEAR, out_view='TABLE_VIEW4')
arcpy.AddJoin_management(in_layer_or_view='ROADS_FOR_JOIN2', in_field='ROAD_JOIN', join_table='TABLE_VIEW4', join_field='ROAD_JOIN', join_type = "KEEP_COMMON")
time.sleep(10)
arcpy.CopyFeatures_management(in_features='ROADS_FOR_JOIN2', out_feature_class= GDB + "\\" + 'ROADS_JOIN_NOT_D14_MCDDA_' + DATE)
### MERGE THE 3
arcpy.management.Merge([GDB + "\\" + 'ROADS_JOIN_D14_' + DATE, GDB + "\\" + 'ROADS_JOIN_MCDDA_' + DATE, GDB + "\\" + 'ROADS_JOIN_NOT_D14_MCDDA_' + DATE], GDB + "\\" + "ROADS_IRRIGATION_USAGE_" + CURRENT_MONTH + YEAR + "_FINAL")

### DELETE UNNESSECARY FIELDS FROM ROADS ###
arcpy.management.DeleteField(GDB + "\\" + "ROADS_IRRIGATION_USAGE_" + CURRENT_MONTH + YEAR + "_FINAL", ["TAB_CAT",
                                                                                                            "MAS_CAT",
                                                                                                           "DIST_PH",
                                                                                                            "ADDRESS",
                                                                                                           "GIS_ACRES",
                                                                                                           "SWE_TYPE1",
                                                                                                           "SWE_TYPE2",
                                                                                                            "SWE_TYPE3",
                                                                                                           "SWE_LABEL",
                                                                                                            "SWE_ID",
                                                                                                           "BOND_REQ",
                                                                                                            "STATUS",
                                                                                                            "EDIT_DATE",
                                                                                                            "DATA_SOURCE",
                                                                                                            "ORACLE",
                                                                                                            "EX_1",
                                                                                                            "EX_2",
                                                                                                            "EX_3",
                                                                                                            "EX_4",
                                                                                                            "EX_5",
                                                                                                            "EX_6",
                                                                                                            "EX_7",
                                                                                                            "EX_8",
                                                                                                            "EX_9",
                                                                                                            "LINK",
                                                                                                            "LINK_DISPLAY",
                                                                                                           "OBJECTID_1",
                                                                                                           "DISTRICT_1",
                                                                                                           "ROAD_JOIN_1",
                                                                                                           "LRP_ID",
                                                                                                           "SWE_TYPE4"], "DELETE_FIELDS")

## DELETE UNNESSECARY FIELDS FROM UNITS ###
arcpy.management.DeleteField(GDB + "\\" + "UNITS_IRRIGATION_USAGE_" + CURRENT_MONTH + YEAR + "_FINAL", ["OBJECTID_1",
                                                                                                           "DISTRICT_1",
                                                                                                           "RES_TYPE5_1"], "DELETE_FIELDS")

units_fc = GDB + "\\" + "UNITS_IRRIGATION_USAGE_" + CURRENT_MONTH + YEAR + "_FINAL"

arcpy.management.AddField(units_fc, "UNIT_JOIN", "TEXT")

fields = ['RES_TYPE5', 'UNIT_JOIN']
with arcpy.da.UpdateCursor(units_fc, fields) as cursor:
    for row in cursor:
        row[1] = str(row[0]).strip()
        cursor.updateRow(row)

roads_fc = GDB + "\\" + "ROADS_IRRIGATION_USAGE_" + CURRENT_MONTH + YEAR + "_FINAL"

# Add UNIT_JOIN field to roads (if not already present)
arcpy.management.AddField(roads_fc, "UNIT_JOIN", "TEXT")

# Make a temporary in-memory layer for spatial join
roads_temp = "in_memory\\roads_with_unit"

# Perform spatial join, bring in RES_TYPE5 from units only
arcpy.analysis.SpatialJoin(
    target_features=roads_fc,
    join_features=units_fc,
    out_feature_class=roads_temp,
    join_operation="JOIN_ONE_TO_ONE",
    join_type="KEEP_COMMON",
    match_option="LARGEST_OVERLAP",
    field_mapping="UNIT_JOIN \"RES_TYPE5\" true true false 255 Text 0 0,First,#," + units_fc + ",RES_TYPE5,0,255"
)

# Build dictionary: {road OBJECTID: RES_TYPE5}
road_unit_dict = {}
with arcpy.da.SearchCursor(roads_temp, ['TARGET_FID', 'UNIT_JOIN']) as cursor:
    for road_id, unit_code in cursor:
        road_unit_dict[road_id] = unit_code.strip() if unit_code else None

# Update original roads with UNIT_JOIN
with arcpy.da.UpdateCursor(roads_fc, ['OBJECTID', 'UNIT_JOIN']) as cursor:
    for row in cursor:
        if row[0] in road_unit_dict:
            row[1] = road_unit_dict[row[0]]
            cursor.updateRow(row)

# Add CUR_USAGE field to roads, homesites, and units 
arcpy.management.AddField(roads_fc, "CUR_USAGE", "LONG")

### EXPORT FINAL FEATURES TO DELIVERABLE GDB ##
CURRENT_MONTH_USAGE = CURRENT_MONTH + "_USAGE"
CURRENT_MONTH_PCT = CURRENT_MONTH + "_PCT"
arcpy.env.workspace = GDB
for fc in arcpy.ListFeatureClasses("*FINAL",'',''):
    arcpy.management.AddField(fc, "CUR_USAGE", "LONG")
    arcpy.management.AddField(fc, "CUR_PCT", "LONG")
    arcpy.management.CalculateField(fc, "CUR_USAGE", f"!{CURRENT_MONTH_USAGE}!", "PYTHON3")
    arcpy.management.CalculateField(fc, "CUR_PCT", f"!{CURRENT_MONTH_PCT}!", "PYTHON3")

    arcpy.CopyFeatures_management(in_features=fc, out_feature_class= r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\00 GEODATABASE\IRRIGATION_USAGE.gdb" + "\\" + fc)

## PART 005: MERGE WITH MOST RECENT CONTRACTORS, USE RESULTS FROM IRRIGATION DATA SCRIPTS USE THE UNIT ENTRIES AND MERGE WITH THE FINAL UNITS OUTPUT SHOULD BE ONLY 4 or 5, FILL IN MISSING FIELDS LIKE UNIT, DISTRICT, UNIT JOIN AFTER MERGE.