"""
===============================================================================
SCRIPT NAME:    003_CREATE_MERGED_DATARAME.py

AUTHOR:         Moises Herrera
CREATED ON:     2025-11-24 (YYYY-MM-DD)
LAST UPDATED:   <Last Update Date> (YYYY-MM-DD)
VERSION:        1.0.0

DESCRIPTION:
    Calculates four month usage for commercial areas and recommended fields and export dataframe to a geodatabase

USAGE:
    Hit run, run in debug mode

REQUIREMENTS:
    - Python version: 3.11.8
    - Dependencies: arcpy, pandas

INPUTS:
    Geodatabase from previous scripts

OUTPUTS:
    Geodatabase with new tables

NOTES:
    Part three in a four part series. Must change current month. 

CHANGELOG:
    2025-08-05 - Moises Herrera: Months are listed in table in reverse chronological order beginnning from current month
    YYYY-MM-DD - <Author>: <Another change>

===============================================================================
"""

### IMPORT ALL MONTHLY GIS TABLES AS DATAFRAMES, MERGE TOGETHER INTO 1 DATAFRAME, CALC 4MONTH USAGE/REC FIELDS, EXPORT MERGED TABLE ###
import arcpy
import pandas as pd
import calendar
from collections import deque
import numpy as np
from datetime import datetime, timedelta

CURRENT_MONTH = (datetime.now() - timedelta(days=30)).strftime("%b").upper()
YEAR = str(int(datetime.now().strftime("%y")))

GDB =  r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\01 MONTHLY RESULTS\GEODATABASE" + "\\" + CURRENT_MONTH + YEAR  + ".gdb"
arcpy.env.workspace = GDB
arcpy.env.overwriteOutput = True
tables = arcpy.ListTables()

#Helper function to help create reverse chronological order in the field headers
def get_last_12_months(current_month_abbr):
    """
    Returns a list of current month abrv in reverse chronological order
    going back 12 months.
    """
    current_month_abbr = current_month_abbr.strip().upper()

    # ['JAN', ..., 'DEC']
    months_abbr = [month.upper() for month in calendar.month_abbr[1:]]
    month_abbr_to_index = {abbr: idx for idx, abbr in enumerate(months_abbr)}  # JAN=0, ..., DEC=11

    if current_month_abbr not in month_abbr_to_index:
        raise ValueError(f"Invalid month abbreviation: {current_month_abbr}")

    current_index = month_abbr_to_index[current_month_abbr]

    result = []
    for i in range(12):
        # Compute index going backwards (with wraparound)
        month_index = (current_index - i) % 12
        month_abbr = months_abbr[month_index]

        result.append(month_abbr)

    return result

## TEMPORARY CHECK AND ADDITION TO GET MOCK DATA
### JAN ###
for table in tables:
    if ("COMMERCIAL_IRRIGATION_USAGE_" in table) and ("COMBINED" not in table) and ("FINAL" not in table) and (table.split("_")[3][:3]) == "JAN":
        columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"] #List the fields you want to include. I want all columns except the geometry
        JAN = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns)
### FEB ###
for table in tables:
    if ("COMMERCIAL_IRRIGATION_USAGE_" in table) and ("COMBINED" not in table) and ("FINAL" not in table) and (table.split("_")[3][:3]) == "FEB":
        columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"] #List the fields you want to include. I want all columns except the geometry
        FEB = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns)
### MAR ###
for table in tables:
    if ("COMMERCIAL_IRRIGATION_USAGE_" in table) and ("COMBINED" not in table) and ("FINAL" not in table) and (table.split("_")[3][:3]) == "MAR":
        columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"] #List the fields you want to include. I want all columns except the geometry
        MAR = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns)
### APR ###
for table in tables:
    if ("COMMERCIAL_IRRIGATION_USAGE_" in table) and ("COMBINED" not in table) and ("FINAL" not in table) and (table.split("_")[3][:3]) == "APR":
        columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"] #List the fields you want to include. I want all columns except the geometry
        APR = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns)
### MAY ###
for table in tables:
    if ("COMMERCIAL_IRRIGATION_USAGE_" in table) and ("COMBINED" not in table) and ("FINAL" not in table) and (table.split("_")[3][:3]) == "MAY":
        columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"] #List the fields you want to include. I want all columns except the geometry
        MAY = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns)
### JUN ###
for table in tables:
    if ("COMMERCIAL_IRRIGATION_USAGE_" in table) and ("COMBINED" not in table) and ("FINAL" not in table) and (table.split("_")[3][:3]) == "JUN":
        columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"] #List the fields you want to include. I want all columns except the geometry
        JUN = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns)
### JUL ###
for table in tables:
    if ("COMMERCIAL_IRRIGATION_USAGE_" in table) and ("COMBINED" not in table) and ("FINAL" not in table) and (table.split("_")[3][:3]) == "JUL":
        columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"] #List the fields you want to include. I want all columns except the geometry
        JUL = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns)
### AUG ###
for table in tables:
    if ("COMMERCIAL_IRRIGATION_USAGE_" in table) and ("COMBINED" not in table) and ("FINAL" not in table) and (table.split("_")[3][:3]) == "AUG":
        columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"] #List the fields you want to include. I want all columns except the geometry
        AUG = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns)
### SEP ###
for table in tables:
    if ("COMMERCIAL_IRRIGATION_USAGE_" in table) and ("COMBINED" not in table) and ("FINAL" not in table) and (table.split("_")[3][:3]) == "SEP":
        columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"] #List the fields you want to include. I want all columns except the geometry
        SEP = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns)
### OCT ###
for table in tables:
    if ("COMMERCIAL_IRRIGATION_USAGE_" in table) and ("COMBINED" not in table) and ("FINAL" not in table) and (table.split("_")[3][:3]) == "OCT":
        columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"] #List the fields you want to include. I want all columns except the geometry
        OCT = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns)
### NOV ###
for table in tables:
    if ("COMMERCIAL_IRRIGATION_USAGE_" in table) and ("COMBINED" not in table) and ("FINAL" not in table) and (table.split("_")[3][:3]) == "NOV":
        columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"] #List the fields you want to include. I want all columns except the geometry
        NOV = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns)
     
### DEC ###
for table in tables:
    if ("COMMERCIAL_IRRIGATION_USAGE_" in table) and ("COMBINED" not in table) and ("FINAL" not in table) and (table.split("_")[3][:3]) == "DEC":
        columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"] #List the fields you want to include. I want all columns except the geometry
        DEC = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns)

### MERGE ALL DATA INTO ONE DF ###
dfm1 = pd.merge(JAN, FEB, how='outer', on='Account_Prefix', suffixes=('_JAN', '_FEB'))
dfm2 = pd.merge(dfm1, MAR, how='outer', on='Account_Prefix', suffixes=('', '_MAR'))
dfm3 = pd.merge(dfm2, APR, how='outer', on='Account_Prefix', suffixes=('', '_APR'))
dfm4 = pd.merge(dfm3, MAY, how='outer', on='Account_Prefix', suffixes=('', '_MAY'))
dfm5 = pd.merge(dfm4, JUN, how='outer', on='Account_Prefix', suffixes=('', '_JUN'))
dfm6 = pd.merge(dfm5, JUL, how='outer', on='Account_Prefix', suffixes=('', '_JUL'))
dfm7 = pd.merge(dfm6, AUG, how='outer', on='Account_Prefix', suffixes=('', '_AUG'))
dfm8 = pd.merge(dfm7, SEP, how='outer', on='Account_Prefix', suffixes=('', '_SEP'))
dfm9 = pd.merge(dfm8, OCT, how='outer', on='Account_Prefix', suffixes=('', '_OCT'))
dfm10 = pd.merge(dfm9, NOV, how='outer', on='Account_Prefix', suffixes=('', '_NOV'))
dfm11 = pd.merge(dfm10, DEC, how='outer', on='Account_Prefix', suffixes=('', '_DEC'))

### CREATE SUBSET OF MERGED DATASET WITH ONLY Account_Prefix AND MONTHLY IRR COlUMNS ###
months_in_order = get_last_12_months(CURRENT_MONTH)

field_list = ["Account_Prefix"]
for m in months_in_order:
    field_list.extend([
        f"{m}_USAGE",
        f"{m}_RMD",
        f"{m}_PCT"
    ])

df_clean_irr = dfm11[field_list]
print("Printing df_cleanirr, all merged dataframes")
print(df_clean_irr)


df_dict = {"JAN": JAN,
          "FEB": FEB,
          "MAR": MAR,
           "APR": APR,
           "MAY": MAY,
           "JUN": JUN,
           "JUL": JUL,
           "AUG": AUG,
           "SEP": SEP,
           "OCT": OCT,
           "NOV": NOV,
            "DEC": DEC}

# print(df_dict["NOV"])
# df_merge = pd.merge(CURRENT_MONTH ,)
df_current_mo = df_dict[CURRENT_MONTH]


### SUBSET CURRENT MONTH TO GET ACCOUNT, CUSTOMER, OTHER INFO ###
df_clean_info = df_current_mo[[
"Account",
"Customer_Name",
"Account_Prefix",
"Customer_Address",
"Irrigation_Usage",
"RMD_USAGE",
"PROVIDER",
"Type"]]

print('--------------------------------------')
print(df_clean_info)
### MERGE SUBSET BACK TO CURRENT MONTH TO GET ACCOUNT, CUSTOMER, OTHER INFO
df_final = pd.merge(df_clean_info, df_clean_irr, how='inner', on='Account_Prefix')
print(df_final)

### MAKE SURE IRRIGATION VALUES ARE FLOAT TYPE ###
astype_dict = {f"{m}_USAGE": "Int64" for m in months_in_order}
astype_dict.update({f"{m}_RMD": "Int64" for m in months_in_order})
astype_dict.update({f"{m}_PCT": "Int64" for m in months_in_order})

df_final = df_final.round().astype(astype_dict, copy=None, errors='ignore')
                                   

### CALCULATE 4 MONTH USAGE FIELDS ###
# Use the first 4 months (current and 3 prior)
first_4 = months_in_order[:4]

df_final['USAGE_4MO'] = sum(df_final[f"{m}_USAGE"].fillna(0) for m in first_4)
df_final['RMD_4MO'] = sum(df_final[f"{m}_RMD"].fillna(0) for m in first_4)
df_final['PCT_4MO'] = ((df_final['USAGE_4MO'] / df_final['RMD_4MO']) * 100).round().astype("Int64")
df_final['RMD_USAGE'] = df_final["RMD_USAGE"].round().astype("Int64").round()
print(df_final)

### WRITE MERGED DATAFRAME TO EXCEL ###
#writer = pd.ExcelWriter(r"A:\GIS\03 PROGRAMMING\0024_IRRIGATION_DASHBOARD\01_DATASTORE\07_TABLES" + "\\"+ CURRENT_MONTH + YEAR + "\\" + "IRRIGATION_USAGE_" + CURRENT_MONTH + YEAR + "_FINAL" + ".xlsx", engine='xlsxwriter')
#df_final.to_excel(writer, CURRENT_MONTH + YEAR ,index=False, startrow=0 , startcol=0)
#writer.save()
file_path = (
    r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\01 MONTHLY RESULTS\COMMERCIAL_TABLES"
    + "\\" + CURRENT_MONTH + YEAR + "\\"
    + "COMMERCIAL_IRRIGATION_USAGE_" + CURRENT_MONTH + YEAR + "_FINAL.xlsx"
)
with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
    # Write df_final to an Excel sheet named with the current month and year (e.g., "0324")
    df_final.to_excel(writer, sheet_name=CURRENT_MONTH + YEAR, index=False, startrow=0, startcol=0)



### CONVERT TABLE TO GDB ###
# table = r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\01 DATA\00 GIS\IRRIGATION.gdb\D14_MATCHES"
arcpy.conversion.ExcelToTable(r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\01 MONTHLY RESULTS\COMMERCIAL_TABLES" + "\\"+ CURRENT_MONTH + YEAR + "\\" + "COMMERCIAL_IRRIGATION_USAGE_" + CURRENT_MONTH + YEAR + "_FINAL" + ".xlsx",
                              r"A:\GIS\01 PROJECTS\906 IRRIGATION USAGE MAP\02 DELIVERABLES\01 MONTHLY RESULTS\GEODATABASE" + "\\" + CURRENT_MONTH + YEAR  + ".gdb" + "\\" + "COMMERCIAL_IRRIGATION_USAGE_" + CURRENT_MONTH + YEAR + "_FINAL")