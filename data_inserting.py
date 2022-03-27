# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 19:34:40 2022

@author: Administrator
"""

from sqlalchemy import create_engine
from openpyxl import load_workbook
import os
import pandas as pd


engine = create_engine('postgresql://postgres:ocpphase01@ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com:5432/offertool')


path='C:/Users/Administrator/Downloads/path_found'

arr = os.listdir(path)

arr.remove("SMBDisEarlyPmt.csv")
arr.remove("SMBDisEarlyPmt_Minibar.csv")
for i in arr:
    df=pd.read_csv(path+'/'+i)
    a=["approver1","appr"]
    name=i.split('.')[0]
    df.to_sql(name=name,con=engine,schema="SMB",if_exists='append',index=False)
    
    