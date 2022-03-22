# -*- coding: utf-8 -*-
"""
Created on Fri Mar  4 12:17:44 2022

@author: subbu
"""

from sqlalchemy import create_engine
import psycopg2
import pandas as pd
import openpyxl
wb = openpyxl.load_workbook("C:/Users/Administrator/Downloads/new_minibar_tables_dummy.xlsx")
sheet_list=wb.sheetnames


engine = create_engine('postgresql://postgres:ocpphase01@ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com:5432/offertool')
con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')



for i in sheet_list:
    df=pd.read_excel("C:/Users/Administrator/Downloads/new_minibar_tables_dummy.xlsx",sheet_name=i)
    df.columns=df.loc[0]
    df.drop(0,inplace=True)
    df.to_csv("C:/Users/Administrator/Documents/"+i+".csv",index=False)
  
    # df.to_sql(i,engine,"SMB",if_exists='append',index=False)
    