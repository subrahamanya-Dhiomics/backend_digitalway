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
        
        cur = con.cursor()


        query='''delete  from "SMB"."SMB - Base Price - Category Addition" where "id"={}'''.format(id_value)
        cur.execute(query)
        con.commit()
        con.close()
        return {"status":"success"},200
    except:
        return {"status":"failure"},500



@app.route('/get_record_baseprice',methods=['GET','POST'])       
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
    
        

@app.route('/update_record_baseprice',methods=['PUT','POST'])
def update_record():
    
    BusinessCode=request.args.get('BusinessCode')
    Market_Country=request.args.get('Market_Country')
    Product_Division=request.args.get('Product_Division')
    Product_Level_02=request.args.get('Product_Level_02')
    Document_Item_Currency=request.args.get('Document_Item_Currency')
    Amount=request.args.get('Amount')
    Currency=request.args.get('Currency')
    id_value=request.args.get('id',type=int)
    
    cur = con.cursor()
    try:
        query='''UPDATE "SMB"."SMB - Base Price - Category Addition" SET 
      "BusinessCode" ={0} ,
     "Market - Country"={1} ,
    	 "Product Division"={2},
     "Product Level 02"={3},
     "Document Item Currency"={4},
     "Amount"={5},
     "Currency"={6}
    	  where "id"={7} '''.format(BusinessCode,Market_Country,Product_Division,Product_Level_02,Document_Item_Currency,Amount,Currency,id_value)
        cur.execute(query)
        con.commit()
        return {"status":"sucess"},200
    
    except:
        return {"status":"failure"},500
    

@app.route('/add_record_baseprice',methods=['POST'])
def add_record():
    
    
    query='''INSERT INTO "SMB"."SMB - Base Price - Category Addition"("id","BusinessCode",
         "Market - Country",
    	 "Product Division",
         "Product Level 02",
         "Document Item Currency",
         "Amount",
         "Currency")
         VALUES(667,'2','2','2','2','2','2','2');'''

    return "hi"        


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

