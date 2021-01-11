#!/usr/bin/env python
# coding: utf-8

# Converter, Jason ops metadata spreadsheet to JSON and XML
# JSON is currently the standard product. XML is a carry-over.
# smccue@whoi.edu
# Version 3- Add paths to sensor files: ctd (ct2), o2 optode (oos),
# magnetmometer (mag or hmr), temperature probe (ctm), depth (dep),
# sound velocity (svp)

import xlrd
#from pandas_ods_reader import read_ods
import pandas as pd
import numpy as np
from datetime import datetime, date
import json
from xml.etree.ElementTree import Element, SubElement, Comment, tostring
from xml.dom import minidom
from collections import defaultdict
import os
#from pyexcel-ods import get_data

######################### Utility Routines ##########################
def etree_to_dict(t):
    d = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {t.tag: {k: v[0] if len(v) == 1 else v
                     for k, v in dd.items()}}
    if t.attrib:
        d[t.tag].update(('@' + k, v)
                        for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
              d[t.tag]['#text'] = text
        else:
            d[t.tag] = text
    return d

def prettify(elem):
    """Return a pretty-printed XML string for an ElementTree structure.
       Copied from the interwebs.
    """
    rough_string = tostring(elem, 'unicode')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")

def to_datestr(inval):
    if (type(inval) == datetime):
        # Not ISO8601, dont be surprised if change needed.
        return(datetime.strftime(inval,"%m/%d/%Y %H:%M:%S"))
    else:
        #return(datetime.strftime(bo_bday_dt,"%m/%d/%Y"))
        return('NaN')
    
################################################################
######################## Main script ###########################
################################################################    
# Define top level dir of data package, which is expected to follow
# a standard hierarchical structure.
# Top level dir is the cruise name, either operator or NDSF name.
# Under the top level dir,
#   - a dir called "Documentation".
#   - a dir called "Vehicle", under which is the standard dirs
#   "Rawdata" and "Procdata".
#
# The metadata spreadsheet must be under the Documentation dir,
# preferred name format is <NDSF_cruisename>_metadata.xls. This
# is not mandatory since the full name is defined below.
#
# The sensor files must be under the Vehicle/Procdata hierarchy.
# Currently the standard path is <op_cruiseID>/<loweringID>/.
#

####################################################################
###################### User Definition Section #####################
####################################################################
#
# Assume tail-end forward slashes are needed in path definitions
# 
#cruisedir_root="C:/users/scotty/Desktop/Projects/NDSF/JasonMGDS/Exercise3_hierarchy/"
#cruisedir_root="D:/TN381/"
#cruiseID_operator="TN381"
#cruiseID_ndsf="TN381_Heintz20"
#metadata_ss_name="Jason_MetaData_TN381_Heintz20.xlsx"

cruisedir_root="D:TN382/"
cruiseID_operator="TN382"
cruiseID_ndsf="TN382_Kawka20"
metadata_ss_name="Jason_Metadata_TN382_Philip_Kawka20.xlsx"

print(metadata_ss_name)
### Output def
json_filename=cruiseID_operator+".json"
outfile=cruisedir_root+json_filename

# Ingest spreadsheet
inss=cruisedir_root+"Documentation/"+metadata_ss_name
print("input spreadsheet is "+inss)
#indf=pd.read_excel(inss,sheet_name="Lowerings",index=False)
#indf=pd.read_excel(inss,sheet_name="Lowerings")
if os.path.exists(inss):
    indf=pd.read_excel(inss, sheet_name="Lowerings", usecols="A:W")
    #sheet_name = "Lowerings"
    #indf=read_ods(inss, sheet_name)
    #data=get_data(inss)
else:
    print("Can't find the spreadsheet file at path "+inss)

# Locate sensor logs, single files for duration of lowering.
procdata_sd=cruisedir_root+"Vehicle/Procdata/"+cruiseID_operator+"/"

# Define the of sensor log types to list in the JSON document. Use
# the labels embedded in the log file names, e.g. "J2-9999.CT2.raw"
sensor_list = {'ctd':'CT2', 'temp_probe':'CTM', 'depth':'DEP', 'magnetometer':'MAG', 'optode':'OOS', 'soundvel':'SVP'}

# Pull the cruise level metadata from the upper block of the sheet.
cr_id=indf[(indf[indf.columns.values[0]]=='Official Cruise Id:')]['Unnamed: 1']
cr_id_str=cr_id.to_string().split()[1]
cs=indf[(indf[indf.columns.values[0]]=='Chief Scientist (s):')]['Unnamed: 1']
cs_str=cs.to_string().split()[1] + " " + cs.to_string().split()[2] # Inflexible, so replace with better code.
vessel=indf[(indf[indf.columns.values[0]]=='Vessel:')]['Unnamed: 1']
op_area=indf[(indf[indf.columns.values[0]]=='Operational Area(s):')]['Unnamed: 1']
cr_startdt=indf[(indf[indf.columns.values[0]]=='Start Date of Cruise:')]['Unnamed: 1']
cr_startdt_str=cr_startdt.to_string().split()[1]
cr_enddt=indf[(indf[indf.columns.values[0]]=='End Date of Cruise:')]['Unnamed: 1']
cr_enddt_str=cr_enddt.to_string().split()[1]

print("Processing " + cr_id_str + " Chief Scientist " + cs_str)

# Carve out the block containing lowering info: IDs, times, coordinates 
lowering_block_row=indf[(indf[indf.columns.values[0]]=='Lowering Id')]

# index yields a type of list, even when one value. Take 1st val.
# Some block headers populate one row up from where 'Lowering Id' is placed, so subtract 1
# in case itll be useful.
block_start=lowering_block_row.index[0]+1 

# count() function gives number fo cells populated in column. The column giving end datetimes
# is empty except where lowering on deck times are entered. Ergo: gives number of lowerings.
low_count=indf.count()['Unnamed: 4']

block_end=block_start+low_count

# Extract sub-dataframe from full frame
lowdf=indf.iloc[block_start:block_end]

# Extract columns of interest from full width dataframe
#lowdf.loc[:,['Unnamed: 0','Unnamed: 1','Unnamed: 2','Unnamed: 3','Unnamed: 4']]

# TBD: find out whether units should be carried along as separate
# entries in the XML tree, as strings added to the values
# pulled from the spreadsheet, or something else.
 

# Developed XML first, but JSON will be the likely format standard.
# XML -> dict -> JSON.


# Cruise level
# =========================================================================
top = Element('cruisedoc')
cr_md = SubElement(top, 'Cruise_Metadata')
crid = SubElement(cr_md, 'cruiseID')
crid.text=cr_id_str
cr_cs = SubElement(cr_md, 'Chief_Scientist')
cr_cs.text=cs_str
cr_startdate = SubElement(cr_md,'StartDate')
cr_startdate.text=cr_startdt_str
cr_stopdate = SubElement(cr_md, 'EndDate')
cr_stopdate.text = cr_enddt_str
 
# Lowering level
llist = SubElement(top, 'Lowerings')
for index, row in lowdf.iterrows():
    l = SubElement(llist, 'Lowering')
    lowID = SubElement(l, 'LoweringID')
    lowID.text = str(row['Unnamed: 0'])
     
    lstartdt = SubElement(l, 'StartDateTime')
    lstartdt.text = to_datestr(row['Unnamed: 1'])
     
    lenddt = SubElement(l, 'EndDateTime')
    lenddt.text = to_datestr(row['Unnamed: 4'])
     
    lsite = SubElement(l, 'SiteDescription')
    lsite.text = str(row['Unnamed: 5'])
     
    lonbotlat = SubElement(l, 'OnBottomLat')
    lonbotlat.text = str(row['Unnamed: 9'])   #was 13
 
    lonbotlon = SubElement(l, 'OnBottomLon')
    lonbotlon.text = str(row['Unnamed: 10'])  #was 14
     
    loffbotlat = SubElement(l, 'OffBottomLat')
    loffbotlat.text = str(row['Unnamed: 12'])  #was 22
     
    loffbotlon = SubElement(l, 'OffBottomLon')
    loffbotlon.text = str(row['Unnamed: 13'])  #was 23
     
    lorglat = SubElement(l, 'Origin_Lat')
    lorglat.text = str(row['Unnamed: 15'])     #was 31
     
    lorglon = SubElement(l, 'Origin_Lon')
    lorglon.text = str(row['Unnamed: 16'])   #was 32
     
    lmaxdepth = SubElement(l, 'MaxDepth')
    lmaxdepth.text = str(row['Unnamed: 17'])  #was 33
     
    lutm = SubElement(l, 'UTMzone')
    lutm.text = str(row['Unnamed: 18'])  #was 34
     
    lwbb = SubElement(l, 'BoundingBoxWest')
    lwbb.text = str(row['Unnamed: 19'])  #was 35
     
    lebb = SubElement(l, 'BoundingBoxEast')
    lebb.text = str(row['Unnamed: 20'])  #was 36
     
    lsbb = SubElement(l, 'BoundingBoxSouth')
    lsbb.text = str(row['Unnamed: 21'])  #was 37
     
    lnbb = SubElement(l, 'BoundingBoxNorth')
    lnbb.text = str(row['Unnamed: 22'])   #was 38
     
    lact = SubElement(l, 'ActivityNote')
    lact.text = str(row['Unnamed: 6'])  # was 39
 
    for sn, st in sensor_list.items():

        sp = procdata_sd +"/"+lowID.text+"/"+lowID.text+"."+st+".raw"
        if os.path.exists(sp):
            print("Adding "+sn+" to the list")
            ste = "PathTo_"+sn+"_file"
            spl = SubElement(l, ste)
            spl.text = sp
            
# Display. TBD: write to file.
# To view the Elementtree XML 
#print(prettify(top)) 
 
# XML to dict
cruise_dict = etree_to_dict(top)
# dict to JSON
cruise_json = json.dumps(cruise_dict)

#pprint(cruise_dict)

with open(outfile, "w") as json_file:
    json_file.write(cruise_json)
json_file.close()