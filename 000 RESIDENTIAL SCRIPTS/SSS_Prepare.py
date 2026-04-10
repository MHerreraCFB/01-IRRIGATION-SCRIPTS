import pandas as pd
import numpy as np
import arcpy
from sqlalchemy import DATE

def prepare_dataframe(df: pd.DataFrame, CURRENT_MONTH:str, YEAR:str):

    arcpy.ClearWorkspaceCache_management()
    arcpy.env.overwriteOutput = True

    #ADD CLEAN 'ROAD' COLUMN TO JOIN HOMESITES WITH ROADS TABLE LATER
    df.rename(columns=lambda x: x.strip(), inplace = True)
    df["ROAD"] = df["Address"].str.strip()

    # Split 'ROAD' once and use the split parts
    road_parts = df["ROAD"].str.split(" ")

    # Extract street name based on number of parts
    df["ROAD"] = np.where(
        road_parts.str.len() > 3,
        road_parts.str[1].str.upper().str.strip() + " " + road_parts.str[2].str.upper().str.strip(),
        road_parts.str[1].str.upper().str.strip()
    )
    
    # Sort by Account
    # Clean up 'Account' and 'Address' fields
    df["Account"] = df["Account"].astype(str).str.strip()
    df = df.sort_values(by=["Account"])

    ### SET NEGATIVE USAGE VALUES TO ZERO ###
    df["Usage"] = np.where(df["Usage"]<0, df["Usage"] == 0, df["Usage"])

    ### REMOVE "S" and "L" PREFIXES
    df.to_csv("frame_before_striping_units.csv")
    df["Unit"] = df["Unit"].astype(str).str.strip().str.lstrip("S").str.lstrip("L")
    df["Unit-Lot"] = df["Unit-Lot"].str.strip().str.lstrip("S").str.lstrip("L")
     
    ### IMPORT TABLE FROM ROADS GIS DATA, WHICH IS SUBSETTED TO ONLY DISTRICT 14 SUBDIVISIONAL ROADS ###
    roads_table = r"A:\GIS\00 DATA\02 GEODATABASES\001 DEVELOPMENT\DEV_ROADWAY.gdb\ROADWAY_FD\ROADWAY_EOP"
    columns = [f.name for f in arcpy.ListFields(roads_table) if f.type!="Geometry"] #List the fields you want to include. I want all columns except the geometry
    expression = "SWE_TYPE3 = 'Subdivision' AND DISTRICT = '14'"
    rds = pd.DataFrame(data=arcpy.da.SearchCursor(roads_table, columns, expression), columns=columns)

    ### ADD CLEAN 'ROAD' COLUMN TO JOIN ROADS TABLE WITH HOMESITES ###
    rds["ROAD"] = rds["LABEL_NAME"].str.strip()
    rds["ROAD"] = np.where(rds["ROAD"].str.split(" ").str.len() > 2, rds["ROAD"].str.split(" ").str[0].str.upper().str.strip() + " " + 
                            rds["ROAD"].str.split(" ").str[1].str.upper().str.strip(), rds["ROAD"].str.split(" ").str[0].str.upper().str.strip())

    ### DROP NAs AND DUPlICATES BEFORE JOIN
    rds.dropna(subset=['ROAD'], inplace = True)
    rds.drop_duplicates(subset=['ROAD'], keep="first", inplace = True)
    rds = rds[["ROAD", "DISTRICT"]]
    df.dropna(subset=['ROAD'], inplace = True)

    print(df)
    print(rds)

    ### RENAME THE 'NAME' COLUMN HEADER TO CUSTOMER_NAME ###
    df.rename(columns = {"Name":"Customer_Name"}, inplace = True)

    df.to_csv("frame_before_road_join.csv", index=False)
    ### MERGE ON ROADS COLUMN TO IDENTIFY DISTRICT 14 HOMESITES, JOIN ON 'LEFT' (IE, HOMESITES) TO KEEP ALl HOMESITES ###
    df_merge = pd.merge(df, rds, how='left', on='ROAD', suffixes=('_1', '_2'))

    df_merge.to_csv("frame_after_road_join.csv", index=False)
    ### APPEND 'L' PREFIX TO ALLKEYS FOR HOMESITES THAT MATCHED D14 ROAD NAME LIST, APPEND 'S' PREFIX TO THE REST ###
    df_merge['RES_ID'] = np.where(df_merge['DISTRICT']== '14', "L" + df_merge["Unit"].astype(str) + "." + df_merge["Lot"].astype(str).str.split('.').str[0], "S" + df_merge["Unit"].astype(str) + "." + df_merge["Lot"].astype(str).str.split('.').str[0])

    ### ADD UNIT SUB IF MISSING IN ORIGINAL TABLE - CHECK AGAINST LIST OF UNITS THAT DON'T HAVE UNIT SUB TO PREVENT INCORRECT UNIT SUB ASSIGNMENT ###  WILL CHANGE IF NOT IN SEWWCA #######################
    no_unit_sub = ['613', '614', '615', '616', '617', '618', '619', '620', '621', '622', '623', '624', '625', '626', '627', '628', '629',
                    '718', '719', '720', '721', '722', '723', '724', '725', '726', '727', '728', '729', '730', '731', '732', '733', '734', '735', '736', '737', '738', '739', '740', '741', '742', '743', '744']
        
    unit_subs = "L|M|F|V|A|B"
        
    # df_merge['RES_ID'] = np.where(~df_merge['UNIT'].isin(no_unit_sub) and)
    df_merge.loc[(~df_merge['Unit'].isin(no_unit_sub)) & (~df_merge['Unit-Lot'].str.contains(unit_subs, case=False)), 'RES_ID'] = "S" + df_merge["Unit"].astype(str) + "V." + df_merge["Lot"].astype(str).str.split('.').str[0]
        
    df_merge.to_csv("frame_after_unit_subs.csv", index=False)
    ### SORT BY ACCOUNT NUMBER SO THE VILLAGES LAND CO WILL COME FIRST FOR HOMES TURNED OVER TO HOMEOWNER FROM DEVELOPER (HELPS WITH NEXT STEP) ###
    df_merge = df_merge.sort_values(by=['RES_ID'])
    df_merge = df_merge.sort_values(by=['Account'])
    df_merge_sort = df_merge.set_index('Account', drop = False)
    print(df_merge_sort)

    ### IMPORT SOD SF TABLE, MERGE TO HOMESITES, HANDLE NON-MATCHES, CALC RECOMMENDED USAGE ###
    sod_table = r"A:\TEST_LOCATION\03 PROGRAMMING\0008_IRRIGATION_USAGE\Irrigation_Scripts_Testing_Data" + "\\" + CURRENT_MONTH + YEAR + "\\" + "MSI102.xlsx"
    sod = pd.read_excel(sod_table, sheet_name='Homesites Total SqFt')
    sod.rename(columns = {"HOMESITE_ALL_KEYS":"RES_ID"}, inplace = True)
    sod.rename(columns = {"TOTAL_SOD_SQUARE_FEET":"SOD_SF"}, inplace = True)
    print(sod)

    ### NON-MATCHES FROM SOD TABLE WILL DROP OUT - NOT USEFUL WITHOUT USAGE ###
    sod_merge = pd.merge(df_merge_sort, sod, how='inner', on='RES_ID', suffixes=('_1', '_2'))
    ### CALC RECOMMENDED USAGE IN GAL/MO FROM SOD SF: SOD_SF*28.3/12*7.48/12 ###  
    sod_merge["RMD_USAGE"] = sod_merge["SOD_SF"]*28.3/12*7.48/12

    ### SORT DF AGAIN AND SET INDEX ###
    sod_merge = sod_merge.sort_values(by=['RES_ID'])
    sod_merge = sod_merge.sort_values(by=['Account'])
    sod_merge_sort = sod_merge.set_index('Account', drop = False)
        
    ### GROUP BY RES_ID TO EFFECTIVELY DISSOLVE AND SUM USAGE AND BASE VALUE FIELDS, WHILE CONCATENATING THE TWO STRINGS FOR NAME AND ACCOUNT FIELDS (lIST NEEDED TO RETAIN ORDER) ###                                                                   
    df_clean = sod_merge_sort.groupby('RES_ID', as_index = False, sort = False).agg({'Account': list,
                                                    'Customer_Name' : list,
                                                'Account Prefix' : 'first',
                                                'Address' : 'first',
                                                'Usage' : 'sum',
                                                'Base' : 'sum',
                                                'Unit' : 'first',
                                                'Lot' : 'first',
                                                'Unit-Lot' : 'first',
                                                'Date' : 'first',
                                                'ROAD' : 'first',
                                                'DISTRICT' : 'first',
                                                'RES_ID' : 'first',
                                                'RMD_USAGE' : 'first',
                                                'SOD_SF' : 'first',
                                                'PROVIDER' : 'first'})

    ### REMOVE QUOTES AND BRACKETS FROM NAME AND ACCOUNT ###
    df_clean["Account"] = df_clean["Account"].astype(str).str.replace(r"[\[\]\"']", "", regex=True).replace(r'^\s*$', np.nan, regex=True)
    df_clean["Account"] = df_clean["Account"].astype(str).str.strip("\"")
    df_clean["Customer_Name"] = df_clean["Customer_Name"].astype(str).str.replace(r"[\[\]\"']", "", regex=True).replace(r'^\s*$', np.nan, regex=True)
    df_clean["Customer_Name"] = df_clean["Customer_Name"].astype(str).str.strip("\"")
    df_clean["Customer_Name"] = df_clean["Customer_Name"].astype(str).str.replace(", ", "/")

    ### CREATE OWNERSHIP COLUMN, CALCULATE VILLAGES OR PRIVATE FOR VALUE BASED ON THE NAME COLUMN AND CONDITIONAL STATEMENT ###
    df_clean['OWNERSHIP'] = 'PRIVATE'
    df_clean.loc[(df_clean["Customer_Name"].str.contains("VILLAGES")) & (~df_clean["Customer_Name"].str.contains("/")), 'OWNERSHIP'] = 'THE VILLAGES'

    ### JOIN TO CLOSING DATE TABLE ###
    closing_date_table = r"A:\TEST_LOCATION\03 PROGRAMMING\0008_IRRIGATION_USAGE\Irrigation_Scripts_Testing_Data" + "\\" + CURRENT_MONTH + YEAR + "\\" + "MSI100.xlsx"
    cdt = pd.read_excel(closing_date_table, 0)
    # RENAME COLUmNS #
    cdt.rename(columns = {"HOMESITE_ALL_KEYS":"RES_ID_CDT"}, inplace = True)
    cdt.rename(columns = {"PHYSICAL_CLOSING_DATE":"CLOSING_DATE"}, inplace = True)
        
    cdt_merge = pd.merge(df_clean, cdt, how='left', left_on='RES_ID', right_on='RES_ID_CDT', suffixes=('_1', '_2'))
    cdt_merge.to_csv("frame_after_closing_date_join.csv", index=False)
    cdt_merge["CLOSING_DATE"] = cdt_merge["CLOSING_DATE"].astype(str)
    df_clean = cdt_merge.copy()
        
    ### ADD EXTRA FIELDS, CALCULATE ###
    df_clean.rename(columns = {"Usage":"Irrigation_Usage"}, inplace = True)
    df_clean['Letters_Sent'] = np.nan
    df_clean['Date_Sent'] = np.nan
    df_clean['COW_Acceptance'] = np.nan
    df_clean[CURRENT_MONTH + '_USAGE'] = np.nan
    df_clean[CURRENT_MONTH + '_RMD'] = df_clean['RMD_USAGE']
    df_clean[CURRENT_MONTH + '_PCT'] = np.nan


    ### UPDATE USAGE AND PCT FOR THAT MONTH ###
    MONTH_USAGE = CURRENT_MONTH + "_USAGE"
    MONTH_PCT = CURRENT_MONTH + "_PCT"
    MONTH_RMD = CURRENT_MONTH + "_RMD"
    df_clean[MONTH_USAGE] = df_clean["Irrigation_Usage"]
    df_clean[MONTH_PCT] = df_clean["Irrigation_Usage"]/df_clean[MONTH_RMD]*100

    print(df_clean)
    return df_clean

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



def prepare_data():
    ### EXPORT HOMESITES TO IRRIGATION GDB
    GDB = r"A:\GIS\00 DATA\02 GEODATABASES\800 SCRATCH_DATA\MH_SCRATCH_DATA\HERRERA_SCRATCH_DATA.gdb"
    arcpy.env.workspace = r"A:\GIS\00 DATA\02 GEODATABASES\001 DEVELOPMENT\DEV_RESIDENTIAL.gdb"
    arcpy.env.overwriteOutput = True
    arcpy.conversion.CopyFeatures('RESIDENTIAL_FD\HOMESITE', GDB + "\\" + "HOMESITE")

    