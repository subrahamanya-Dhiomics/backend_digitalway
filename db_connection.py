# -*- coding: utf-8 -*-
"""
Created on Wed Mar  2 15:32:15 2022

@author: Administrator
"""
import pymssql
import pandas as pd
con = pymssql.connect(
    host = "ocpphasesql1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com",
    user = "admin",
    password = "ocpphasesql1"
    ,database='OCPPhasePostgresProd'
    )

df=pd.read_sql('select * from dbo.ordertoofferV2',con=con)



