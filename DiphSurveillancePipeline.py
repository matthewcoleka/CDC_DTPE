# %%
#Dependencies
import http.client
import pandas as pd
import json
import numpy as np
import datetime as dt
import os
import pyodbc
import openpyxl
import sqlalchemy as db
from keys import keys

# %%
#Set Up Connections
########## EpiInfo ##########
conn = http.client.HTTPSConnection("epi-info-data-service.services.cdc.gov")
conn.set_debuglevel(1)
headers = {
    'authtoken': keys['epi_info_key'],
    'accept': "*/*"
    }
########## PDL Database ##########
#Connect to PDL database (SQL SERVER through ODBC)
pdl_engine = db.create_engine("mssql+pyodbc://@Pertussis Lab Database", use_setinputsizes=False)

# #Initiate SQLAlchemy Metadata and table objects
metadata = db.MetaData()
ccrf_tbl = db.Table(
    'CCRF', 
    metadata,
    autoload_with=pdl_engine
)
# coryne = db.Table(
#     'coryne', 
#     metadata,
#     autoload_with=pdl_engine
# )
# input = db.Table(
#     'Input', 
#     metadata,
#     autoload_with=pdl_engine
# )

# %%
#Pull in CCRF data
conn.request("POST", "/api/SurveyData/GetSurveyData?surveyid=0c8b2402-03a4-4599-824c-2bd3a0fe6ab6", headers=headers)

res = conn.getresponse()
r = res.read()
data=json.loads(r.decode("utf-8"))
ccrf_raw=pd.DataFrame(data)
pd.set_option('display.max_columns', 500)

# %%
#PDL Data Clean
#Select All PC IDs, add metadata
input_query = "SELECT * FROM INPUT WHERE country ='United States' and ID like 'PC%'"
input_df = pd.read_sql_query(input_query, pdl_engine)
coryne_df = pd.read_sql_table('coryne', pdl_engine)
coryne_ast_df = pd.read_sql_table('coryne_ast', pdl_engine)

# %%
#Clean Input
#input_df.columns
input_keep_cols=[
    'ID',
    'acc_num',
    'age_lab',
    'age_units',
    'daterec',
    'datecol',
    'state_lab',
    'spectype2',
    'specsite',
    'clin_summary',
    'organization',
    'sex'
]
input_final = input_df[input_keep_cols].sort_values('ID',ascending=False)
#Clean Age
input_final.loc[:,'age_lab'] = pd.to_numeric(input_final['age_lab'], errors='coerce').astype('float')
#input_final.head()

# %%
#Clean Coryne Results
#coryne_df.columns
coryne_keep_cols=[
    'cdcid',
    'toxigenic',
    'cryne_ov_interp',
    'coryne_pcr_interp',
    'biotype'
]
coryne_clean = coryne_df[coryne_df['cdcid'].str.startswith('PC')][coryne_keep_cols].sort_values('cdcid',ascending=False)
# Age

#Culture Recode
coryne_clean.loc[:,'cryne_ov_interp'] = coryne_clean['cryne_ov_interp'].map({
    10:1,
    11:2,
    12:3,
    13:4,
    7:5,
    8:6,
    88:7,
    9:999
})

#AST
ast_keep_cols = [col for col in coryne_ast_df if (col.endswith('_int')) or (col=='cdcid')]
#Merge Coryne with AST results
coryne_final = pd.merge(left = coryne_clean, right=coryne_ast_df[ast_keep_cols], left_on='cdcid',right_on='cdcid',how='left')
#coryne_clean.head()

# %%
#Merge Lab Data
pdl_df= pd.merge(left=input_final, right=coryne_final, left_on='ID',right_on='cdcid',how='left')
pdl_final= pdl_df.drop(['cdcid'],axis=1).rename({'ID':'cdcid'},axis=1)
#Rename Cols
pdl_final = pdl_final.rename(columns={
    "cryne_ov_interp":"CDC_CULT",
    "coryne_pcr_interp":"CDC_PCR",
    "biotype":"CDC_BIOTYPE",
    "toxigenic":"CDC_TOXIGENIC",
    "daterec":"CDC_DATEREC",
    "datecol":"CDC_DATECOL",
    "sex":"CDC_SEX",
    "spectype2":"CDC_SPECTYPE",
    "specsite":"CDC_SPECSITE",
    "coryne_pen_int":"CDC_PEN",
    "coryne_mero_int":"CDC_MERO",
    "coryne_vanc_int":"CDC_VANC",
    "coryne_dapt_int":"CDC_DAPT",
    "coryne_azit_int":"CDC_AZIT",
    "coryne_eryt_int":"CDC_ERYT",
    "coryne_clar_int":"CDC_CLAR",
    "coryne_levo_int":"CDC_LEVO",
    "coryne_oflo_int":"CDC_OFLO",
    "coryne_clin_int":"CDC_CLIN",
    "coryne_rifa_int":"CDC_RIFA",
    "coryne_amox_int":"CDC_AMOX"
})

#pdl_final.head()

# %% [markdown]
#  Clean CCRF - Remove dummy data (GAEXAMPLE), empty responses (state is null), records marked for removal (stateid = DELETE)

# %%
ccrf_df=ccrf_raw[(ccrf_raw['RecordID'] != 'GAEXAMPLE') & (ccrf_raw['state'].notnull()) & (ccrf_raw['StateID'] != 'DELETE')]

# %% [markdown]
# Clean CCRF - Remove records marked as duplicates

# %%
dm_path = r"C:\Users\orv2\CDC\NCIRD-MVPDB-DTP-EPI - Documents\Diphtheria\Surveillance\Data Management"
dups_remove = pd.read_excel(dm_path+"\CCRF_Duplicates.xlsx")
ccrf_df = ccrf_df[~ccrf_df['GlobalRecordId'].isin(dups_remove['GlobalRecordID'])]

# %% [markdown]
# Clean CCRF - Remove unused columns and rename some columns

# %%
ccrf_df = ccrf_df.drop(['_ParentRecordId'],axis=1).rename({
    "_DateUpdated":"EI_DateUpdated",
    "_Status":"EI_Status",
    "GlobalRecordId":"EI_RecordID"
}, axis = 1)

# %% [markdown]
# Clean CCRF - Find duplicates with record IDs over length 2, and keep the record with complete status

# %%
#Grab Record ids
rids = ccrf_df['RecordID'].value_counts().to_frame().reset_index()
#Select IDs that have length over 2 and have multiple entries
dups = rids[(rids['RecordID'].str.len()>2) & (rids['count']>1)]['RecordID']
#Find Global Record ID of duplicate ID that is listed as not 'Complete'
dups_to_drop = ccrf_df[(ccrf_df['RecordID'].isin(dups)) & (ccrf_df['EI_Status']!='Complete')]['EI_RecordID']
# Drop incomplete dup records   
ccrf_df_final = ccrf_df[~ccrf_df['EI_RecordID'].isin(dups_to_drop)]

# %%
#Clean Variable Types
#Booleans
bool_cols = ccrf_df_final.select_dtypes(bool).columns
ccrf_df_final.loc[:,bool_cols] = ccrf_df_final[bool_cols].astype("Int64")


# %% [markdown]
# Dates

# %%
date_cols = ['AmoxDOI',
'Arrival1',
'Arrival2',
'Arrival3',
'AzithroDOI',
'CephaDOI',
'CiproDOI',
'ClarithroDOI',
'datecol1',
'datecol2',
'datecol3',
'datelastvax',
'datevax1',
'datevax2',
'datevax3',
'datevax4',
'datevax5',
'datevax6',
'datevax7',
'datevax8',
'Departure1',
'Departure2',
'Departure3',
'ErythroDOI',
'OtherDOI',
'PenDOI',
'ReportDate',
'TetraDOI',
'TrimetDOI',
'UnkDOI',
'VancoDOI']
for col in date_cols:
    ccrf_df_final.loc[:,col]=pd.to_datetime(ccrf_df_final[col],format="%Y-%m-%d", errors='coerce').dt.date

# %% [markdown]
# Convert from Char to Int

# %%
#AgeType
ccrf_df_final.loc[:,'agetype'] = ccrf_df_final['agetype'].map({
    "Years":1,
    "Months":2,
    "Weeks":3,
    "Days":4
}).astype("Int64")
#Sex
ccrf_df_final.loc[:,'Sex'] = ccrf_df_final['Sex'].map({
    "Man":1,
    "Woman":2,
    "Transgender Woman":3,
    "Transgender Man":4,
    "Prefer not to answer":5,
    "Unknown":999
}).astype("Int64")
#Submitter Species
for i in range(1,4):
    var = 'species'+str(i)
    ccrf_df_final.loc[:,var] = ccrf_df_final[var].map({
        "C. diphtheriae":1,
        "C. ulcerans":2,
        "C. pseudotuberculosis":3,
        "Corynebacterium spp.":4,
        "Other":5,
        "Unknown":999
    }).astype("Int64")
#YNU Cols
ynu_cols = ["AST","CaseExposure","Polymicrobial","UnpasteurizedDairy","vaccine"]
for col in ynu_cols:
    ccrf_df_final.loc[:,col] = ccrf_df_final[col].map({
        "Yes":1,
        "No":2,
        "Unknown":999
    }).astype("Int64")
#Dispo
ccrf_df_final.loc[:,'Dispo'] = ccrf_df_final['Dispo'].map({
    "Inpatient":1,
    "Outpatient":2,
    "Discharged":3,
    "Deceased":4,
    "Left Against Medical Advice (AMA)":5,
    "Other": 6,
    "Unknown":999
}).astype("Int64")
#Housing
ccrf_df_final.loc[:,'Housing'] = ccrf_df_final['Housing'].map({
    "Not experiencing homelessness":1,
    "Person experiencing homelessness":2,
    "Previous history of homelessness":3,
    "Unknown":999
}).astype("Int64")
#IVDU
ccrf_df_final.loc[:,'IVDU'] = ccrf_df_final['IVDU'].map({
    "No drug use":1,
    "Current IVDU":2,
    "Previous history of IVDU":3,
    "Other substance abuse (e.g., alcohol, non-IVDU substance use)": 4,
    "Unknown":999
}).astype("Int64")
#Travel1
ccrf_df_final.loc[:,'Travel1'] = ccrf_df_final['Travel1'].map({
    "No - No Domestic/International Travel":1,
    "Yes - Domestic/Interstate Travel":2,
    "Yes - International Travel":3,
    "Yes - Both Domestic and International Travel": 4,
    "Unknown":999
}).astype("Int64")
#Status
ccrf_df_final.loc[:,'EI_Status'] = ccrf_df_final['EI_Status'].map({
    "Complete":1,
    "In Process":2,
    "In Progress (URL)":3
}).astype("Int64")

# %% [markdown]
# Vaxtype data error

# %%
vtype_cols = ['vaxtype1','vaxtype2','vaxtype3','vaxtype4','vaxtype5','vaxtype6','vaxtype7','vaxtype8','typelastvax']
for col in vtype_cols:
    ccrf_df_final.loc[ccrf_df_final[col]=='1-DT or Td (e.g., Tenivac)',col]=1
    ccrf_df_final.loc[ccrf_df_final[col]=='2-DTP',col]=2
    ccrf_df_final.loc[ccrf_df_final[col]=='3-DTP-Hib',col]=3
    ccrf_df_final.loc[ccrf_df_final[col]=='4-DTP-Hib-HepB',col]=4
    ccrf_df_final.loc[ccrf_df_final[col]=='5-DTaP (e.g., Daptacel, Infanrix)',col]=5
    ccrf_df_final.loc[ccrf_df_final[col]=='6-DTaP-IPV (e.g., Kinrix, Quadracel)',col]=6
    ccrf_df_final.loc[ccrf_df_final[col]=='7-DTaP-IPV-Hep B (e.g., Pediarix)',col]=7
    ccrf_df_final.loc[ccrf_df_final[col]=='8-DTaP-IPV-Hib (e.g., Pentacel)',col]=8
    ccrf_df_final.loc[ccrf_df_final[col]=='9-DTaP-IPV-Hib-HepB (e.g., Vaxelis)',col]=9
    ccrf_df_final.loc[ccrf_df_final[col]=='10-Tdap (e.g., Boostrix, Adacel)',col]=10
    ccrf_df_final.loc[ccrf_df_final[col]=='11-Other (e.g. unspecified diphtheria toxoid-containing vaccine)',col]=11
    ccrf_df_final.loc[ccrf_df_final[col]=='99-Unknown',col]=99
    ccrf_df_final.loc[:, col] = pd.to_numeric(ccrf_df_final[col], errors='coerce',).astype("Int64")

# %%
#Antibiotic Duration
dura_cols = ccrf_df_final.loc[:, ccrf_df_final.columns.str.endswith('Dura')].columns
for col in dura_cols:
    ccrf_df_final.loc[:, col] = pd.to_numeric(ccrf_df_final[col], errors='coerce',).astype("Int64")

# %% [markdown]
# Merge CCRF and PDL - BY SPHLID

# %%
#Create copy for merging
merged = ccrf_df_final.copy()
#Intitate list to capture merged isolates
iso_matched = []
#Iterate through sphlid1-sphlid3 and merge on acc_num
for i in range(1,4):
    acc_num = 'acc_num'+str(i)
    sphlid = 'sphlid'+str(i)
    cdcid = 'cdcid'+str(i)
    pdl_merge = pdl_final.add_suffix(str(i)).dropna(subset=[acc_num])
    merged = pd.merge(left=merged, right=pdl_merge,left_on=sphlid,right_on=acc_num,how='left')
    #Collect matched isolates
    [iso_matched.append(cdcid) for cdcid in merged[merged[acc_num].notnull()][cdcid]]

# %% [markdown]
# Merge CCRF and PDL - By Common attributes

# %%
# CCRF - Find unmatched records
matched_ccrf = merged[merged['cdcid1'].notnull()]['EI_RecordID']
epi_remain=ccrf_df_final[~merged['EI_RecordID'].isin(matched_ccrf).values].copy()
# CCRF - Drop rows with missing data
epi_remain = epi_remain.dropna(subset=['Age','state','datecol1'], how='any')
# CCRF - Change cities back to states
epi_remain['state'] =  epi_remain['state'].str.replace("LAC","CA")
epi_remain['state'] =  epi_remain['state'].str.replace("NYC","NY")
epi_remain['state'] =  epi_remain['state'].str.replace("PHI","PA")

for i in range(1,4):
    x=str(i)
    # PDL - Find unmatched records and add suffix to concat to cdcid1
    pdl_remain = pdl_final[~pdl_final['cdcid'].isin(iso_matched)].add_suffix(x)

    # PDL - Drop rows with missing data
    pdl_remain = pdl_remain.dropna(subset=['age_lab'+x,'state_lab'+x,'CDC_DATECOL'+x], how='any')

    # Merge on State, Age, and Date Cx, first merge is an inner join, second and third merges are left joins (funky)
    if i == 1:
        secondary_merge = pd.merge(left = epi_remain, right=pdl_remain, left_on=['Age','state','datecol1'], right_on=['age_lab'+x,'state_lab'+x,'CDC_DATECOL'+x], how='inner')
    else:
        secondary_merge = pd.merge(left = secondary_merge, right=pdl_remain, left_on=['Age','state','datecol1'], right_on=['age_lab'+x,'state_lab'+x,'CDC_DATECOL'+x], how='left')
    
    # Drop duplicated rows, keep first match
    secondary_merge=secondary_merge.drop_duplicates(subset=['EI_RecordID'], keep='first')

    # Collect matched isolates from secondary merge
    [iso_matched.append(cdcid) for cdcid in secondary_merge['cdcid'+x] if cdcid not in iso_matched]

# %%
### Update primary merged df with secondary merged df
#Change states back to cities
secondary_merge.set_index('EI_RecordID', inplace=True)
secondary_merge['state'].update(ccrf_df_final.set_index('EI_RecordID')['state'])

# Update EI records from secondary merge 
merged.set_index('EI_RecordID', inplace=True)
merged.update(secondary_merge,join='left', overwrite=True)

# Reset Index
merged=merged.reset_index(level=['EI_RecordID'])

# %% [markdown]
# Add unmatched PDL isolates to merged dataframe

# %%
#Isolate remaining isolates and add suffix to concat to cdcid1
pdl_remain = pdl_final[~pdl_final['cdcid'].isin(iso_matched)].add_suffix('1')
#Add remaining isolates to merged df
final_df = pd.concat([merged,pdl_remain])

# %% [markdown]
# Add System Variables

# %%
#Add system variables

#Source
final_df.loc[final_df['cdcid1'].notna(),'system_source']='Lab Only'
final_df.loc[final_df['EI_RecordID'].notna(),'system_source']='CCRF Only'
final_df.loc[(final_df['EI_RecordID'].notna()) & (final_df['cdcid1'].notna()),'system_source']='Combined'

#Add Integer Date Columns for PowerApps
#(YEAR([date]) * 10000 + MONTH([date]) * 100 + DAY([date]))
final_df['system_datecol'] = pd.to_datetime(final_df[['datecol1','CDC_DATECOL1']].bfill(axis=1).iloc[:,0]).dt.year*10000 + pd.to_datetime(final_df[['datecol1','CDC_DATECOL1']].bfill(axis=1).iloc[:,0]).dt.month*100 + pd.to_datetime(final_df[['datecol1','CDC_DATECOL1']].bfill(axis=1).iloc[:,0]).dt.day
final_df['system_daterec'] = pd.to_datetime(final_df[['ReportDate','CDC_DATEREC1']].bfill(axis=1).iloc[:,0]).dt.year*10000 + pd.to_datetime(final_df[['ReportDate','CDC_DATEREC1']].bfill(axis=1).iloc[:,0]).dt.month*100 + pd.to_datetime(final_df[['ReportDate','CDC_DATEREC1']].bfill(axis=1).iloc[:,0]).dt.day

#Mark Epi Duplicates
final_df.loc[(final_df['RecordID'].duplicated(keep=False)) & (final_df['RecordID'].notna()) & (final_df['StateID'].notna()),'system_source']='Duplicated'
# Mark Lab Duplicates
final_df.loc[(final_df['cdcid1'].duplicated(keep=False)) & (final_df['cdcid1'].notna()),'system_source']='Duplicated'

#print(final_df['system_source'].value_counts())

#Pipeline Run Datetime
final_df['system_datetime']=dt.datetime.now()


# %% [markdown]
# Send email to Farrell of unmatched isolates

# %%
#Isolate unmatched
unmatch_ccrf_df = final_df[final_df['system_source']=='CCRF Only'][[
    "EI_RecordID",
    "state",
    "RecordID",
    "ReportDate",
    "poc_email",
    "Sex",
    "Age",
    "agetype",
    "datecol1",
    "sphlid1",
    "collectsite1"
]]
unmatch_lab_df = final_df[final_df['system_source']=='Lab Only'][[
    "cdcid1",	
    "acc_num1",	
    "age_lab1",	
    "age_units1",
    "CDC_DATEREC1",	
    "CDC_DATECOL1",	
    "state_lab1",	
    "CDC_SPECTYPE1",	
    "CDC_SPECSITE1",	
    "clin_summary1",	
    "organization1",
    "CDC_TOXIGENIC1"
]]
dups_df = final_df[final_df['system_source']=='Duplicated']

#Filter date and column criteria
current_date = dt.datetime.now().date()
#date_cutoff = current_date+dt.timedelta(days=-30)
date_cutoff = dt.date(2024,1,1)
unmatch_lab_df=unmatch_lab_df[(unmatch_lab_df['CDC_DATEREC1']>= date_cutoff) & (unmatch_lab_df['CDC_TOXIGENIC1'].notna())].drop(['CDC_TOXIGENIC1'], axis=1)



# %% [markdown]
# Create Quality File

# %%
import getpass
user_name = getpass.getuser()
base_path=r"C:\Users"
sharepoint_folder = r"CDC\NCIRD-MVPDB-DTP-EPI - Documents\Diphtheria\Surveillance\Data Management\Quality"
file_ext=".xlsx"
filename = "DiphtheriaSurveillanceQualityReport_"+dt.datetime.now().strftime('%d%b%Y_%H%M')+file_ext
outpath = os.path.join(base_path,user_name,sharepoint_folder,filename)

#Create Excel
with pd.ExcelWriter(outpath) as writer:
    if len(dups_df)>0:
        dups_df.to_excel(writer, sheet_name="Duplicates",index=False)
    if len(unmatch_ccrf_df)>0:
        unmatch_ccrf_df.to_excel(writer, sheet_name="Unmatched CCRF",index=False)
    if len(unmatch_lab_df)>0:
        unmatch_lab_df.to_excel(writer, sheet_name="Unmatched Lab",index=False)

# %% [markdown]
# Email Group

# %%
# Send Email to Group 
import win32com.client

def send_outlook_html_mail(recipients, attachment, subject='No Subject', body='Blank', send_or_display='Display', copies=None):
    """
    Send an Outlook HTML email
    :param recipients: list of recipients' email addresses (list object)
    :param subject: subject of the email
    :param body: HTML body of the email
    :param send_or_display: Send - send email automatically | Display - email gets created user have to click Send
    :param copies: list of CCs' email addresses
    :return: None
    """
    if len(recipients) > 0 and isinstance(recipient_list, list):
        outlook = win32com.client.Dispatch("Outlook.Application")
        ol_msg = outlook.CreateItem(0)
        str_to = ""
        for recipient in recipients:
            str_to += recipient + ";"
        ol_msg.To = str_to
        if copies is not None:
            str_cc = ""
            for cc in copies:
                str_cc += cc + ";"
            ol_msg.CC = str_cc
        ol_msg.Subject = subject
        ol_msg.HTMLBody = body
        ol_msg.Attachments.Add(attachment)
        if send_or_display.upper() == 'SEND':
            ol_msg.Send()
        else:
            ol_msg.Display()
    else:
        print('Recipient email address - NOT FOUND')      

# Hard coded email subject
MAIL_SUBJECT = f"Diphtheria Surveillance Quality Report {current_date.strftime('%d%b%Y')}"

# Hard coded email HTML text
MAIL_BODY_FINAL ="""<html><body><p>Please see attached today's Diphtheria Surveillance Quality Report</p>"""

recipient_list = ['oqk3@cdc.gov']
copies_list = ['orv2@cdc.gov']

#Use integer division to get week number
day_of_month = dt.datetime.now().day
week_number = (day_of_month-1)//7+1

#Get weekday - 0 = Monday / 6 = Sunday
week_day = dt.datetime.now().weekday()

#Send email on 2nd and 4th Monday each month
if (week_number in(2, 4)) & (week_day == 0):
    #Send email
    send_outlook_html_mail(recipients=recipient_list, attachment=outpath, subject=MAIL_SUBJECT, body=MAIL_BODY_FINAL, send_or_display='Send', copies=copies_list)

# %% [markdown]
# Update PDL Database

# %%
#Delete All then Add daily?
stmt = db.delete(ccrf_tbl)
with pdl_engine.begin() as conn:
    conn.execute(stmt)
#How to handle historic data?
final_df.to_sql("CCRF", pdl_engine, if_exists='append',index=False, chunksize=75, method=None)


