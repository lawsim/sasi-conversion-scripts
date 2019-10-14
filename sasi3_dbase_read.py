#!/usr/bin/env python
# coding: utf-8

# In[43]:


from dbfread import DBF
import pandas as pd
import numpy as np
import glob
import os
import re
from collections import OrderedDict
import io
import struct
from string import strip


# In[37]:


years = {
    '0' : "1990-1991",
    '1' : "1991-1992",
    '2' : "1992-1993",
    '3' : "1993-1994",
    '4' : "1994-1995",
    '5' : "1995-1996",
    '6' : "1996-1997",
    '7' : "1997-1998",
    '8' : "1998-1999",
    '9' : "1999-2000",
}

schools = {
    '01' : "Bunker",
    '02' : "Graham",
    '03' : "Kennedy",
    '04' : "Lincoln",
    '05' : "Milani",
    '07' : "Musick",
    '09' : "Schilling",
    '10' : "Snow",
    '20' : "New Beginnings",
    '22' : "NJHS",
    '30' : "Progressive Academy",
    '31' : "NMHS",
    '50' : "Bridgepoint",
    '65' : "Crossroads",
}

print(years, schools)


# In[38]:


savecols = ['PERMNUM',
'ENTERDATE',
'LEAVEDATE',
'ENTERCODE',
'LEAVECODE',
'LASTNAME',
'FIRSTNAME',
'MIDDLENAME',
'BIRTHDATE',
'GENDER',
'GRADE',
'PRNTGUARD',
'MAILADDR',
'CITY',
'ZIPCODE',
'STATE',
'TELEPHONE',
'BIRTHPLACE']


# In[57]:


astu_files = glob.glob('./sasi3_data_in/STU[0-9][0-9][0-9].DAT')
#for fname in astu_files:
    #print(os.path.basename(fname))
    
    #break


# In[61]:


outrs = []

for filepath in astu_files:
    #filename = 'ASTU3065.DBF'
    filename = os.path.basename(filepath)
    
    # get year/school from filename
    pattern = 'STU(\d)(\d\d)\.DAT'
    matches = re.match(pattern, filename)
    year = matches.group(1)
    school = matches.group(2)

    if year not in years or school not in schools:
        print(year, school, " not found")
        continue
    
    # read file in and split to lines
    inf = open(filepath, 'r')
    line = inf.read()
    n = 512
    all_lines = [line[i:i+n] for i in range(0, len(line), n)]

    # go through each line, add to list for dataframe
    for line in all_lines:
        if line[0:4] == "0000":
            continue

        outr = OrderedDict()
        outr['SOURCE_PROG'] = 'SASI3'
        outr['SOURCE_YEAR'] = years[year]
        outr['SOURCE_FILE'] = filename
        outr['SOURCE_SCHOOL'] = schools[school]

        for col in savecols:
            outr[col] = ""

        outr['STUNUM'] = line[0:4].strip()
        outr['LASTNAME'] = line[4:20].strip()
        outr['FIRSTNAME'] = line[20:33].strip()
        outr['MIDDLENAME'] = line[33:46].strip()
        outr['PERMNUM'] = line[46:56].strip()
        outr['GENDER'] = line[56:57].strip()
        outr['GRADE'] = line[57:59].strip()
        outr['BIRTHDATE'] = line[59:65].strip()
        outr['PRNTGUARD'] = line[65:89].strip()
        outr['MAILADDR'] = line[89:113].strip()
        outr['CITY'] = line[113:131].strip()
        outr['STATE'] = line[131:133].strip()
        outr['ZIPCODE'] = line[133:142].strip()
        outr['TELEPHONE'] = line[142:152].strip()
        outr['ENTERDATE'] = line[193:199].strip()
        outr['LEAVEDATE'] = line[199:205].strip()
        outr['BIRTHPLACE'] = line[331:351].strip()

        outrs.append(outr)

df_records = pd.DataFrame(outrs)


# In[63]:


df_records.head()

df_records.to_excel('out_data/sasi3out.xlsx')

