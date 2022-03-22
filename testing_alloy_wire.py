# -*- coding: utf-8 -*-
"""
Created on Fri Mar  4 14:41:41 2022

@author: Administrator
"""

from sqlalchemy import create_engine
import getpass
from datetime import datetime
from datetime import date
import numpy as np
import pandas as pd

import pymssql
import pandas as pd

consql = pymssql.connect(
    host = "ocpphasesql1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com",
    user = "admin",
    password = "ocpphasesql1"
    ,database='OCPPhasePostgresProd'
    )

today = date.today()


stock_df=pd.read_excel('C:/Users/Administrator/Documents/alloy_wire/Alloy Surcharge_wire.xlsx')
  # filtering only hamburg and duisburg data
stock_df=stock_df[(stock_df['Mill']=='Duisburg') | (stock_df['Mill']=='Hamburg')]



# selecting and renaming the columns
data1=stock_df[['Month/Year', 'Monthly Alloy Surcharge','Customer ID','Internal Grade','Mill']]
data1=data1.rename(columns={"Month/Year": "Month_year", "Monthly Alloy Surcharge": "Amount","Customer ID":"Customer_ID","Internal Grade":"Internal_Grade"})

# adding required columns with constant values
VKORG="0300"
DST_CH="02"
DIV="02"
COND_TYPE="Z133"
data1.insert(0,'VKORG',str(VKORG))
data1.insert(1,'COND_TYPE',str(COND_TYPE))
data1.insert(2,'DST_CH',str(DST_CH))
data1.insert(3,'DIV',str(DIV))
data1['VKORG'][data1.Mill == "Hamburg"] ="0400"


# filling the zeros & dropping the null values
data1=data1.dropna()
data1['Internal_Grade']=data1["Internal_Grade"].astype(str)
data1['Internal_Grade']=data1['Internal_Grade'].apply(lambda x: x.zfill(4))
   
# filtering only current month and next month data

data1['Month_year']=data1['Month_year'].astype(str)
data1["Month_year"] =data1["Month_year"].str.replace("_", "")
data1['Month_year'] = data1['Month_year'].str.split('.').str[0]
data1=data1[(pd.to_datetime(data1['Month_year'], format='%Y%m')) =='2022-01' ]
    
        
# df=pd.read_sql('select * from dbo.ordertoofferV2',con=con)

data1.to_excel('data1.xlsx')
# df.to_excel('df.xlsx')

s=pd.DataFrame()
# for i in range(1,len(data1)):
    
#      s.append(df.loc[df['accountcode']==data1['Customer_ID'][i] & df['grade']==data1['Internal_Grade'][i]])
 



