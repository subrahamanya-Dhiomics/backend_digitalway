# -*- coding: utf-8 -*-
"""
Created on Thu Oct  7 07:33:38 2021

@author: Administrator
"""


import numpy as np
import pandas as pd
import traceback
import time
import json
from flask import Flask, request, render_template
from flask import jsonify
from flask_cors import CORS
from json import JSONEncoder
from collections import OrderedDict
from flask import Blueprint
import calendar
import psycopg2
import shutil

from pathlib import Path
import os
from sqlalchemy import create_engine
import getpass
from datetime import datetime
from datetime import date
import random



app = Flask(__name__)
CORS(app)

input_directory="C:/Users/Administrator/Documents/SMB_INPUT/"


con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')
cur = con.cursor()





@app.route('/Base_Price_Data',methods=['GET','POST'])
def SMB_data():
   
    search_string=request.args.get("search_string")
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    try:
        search_string=int(search_string)
    except:
        search_string=search_string
        
    
    
    try:
    
        df = pd.read_sql('''select * from "SMB"."SMB - Base Price - Category Addition" order by "id" desc''', con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
    
        count=len(df)
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        return {"data":table,"totalCount":count},200
   
    except:
        return {"statuscode":500,"msg":"failure"},500
        
       

  
@app.route('/delete_record_baseprice',methods=['POST','GET','DELETE'])
def delete_record():
    id_value=request.args.get('id')
    
    try:
        
        query='''delete  from "SMB"."SMB - Base Price - Category Addition" where "id"={}'''.format(id_value)
        cur.execute(query)
        con.commit()
        con.close()
        return {"status":"success"},200
    except:
        return {"status":"failure"},500

@app.route('/get_record_baseprice')       
def get_record():
    id_value=request.args.get('id')
       
    query='''SELECT * FROM "SMB"."SMB - Base Price - Category Addition" where id={}'''.format(id_value)

    try:
        
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500

@app.route('/Base_Price_Upload', methods=['GET','POST'])
def  SMB():
    
    f=request.files['filename']
    try:
            
        f.save(input_directory+f.filename)
        smb_df=pd.read_excel(input_directory+f.filename)
        table=json.loads(smb_df.to_json(orient='records'))
        
        return {"data":table},200
    except:
        return {"statuscode":500,"message":"incorrect"},500
 


if __name__ == '__main__':
    app.run()

