# -*- coding: utf-8 -*-
"""
Created on Wed Nov 10 07:21:49 2021

@author: Administrator
"""
from flask import Blueprint


import pandas as pd
import time
import json
from flask import Flask, request, send_file, render_template, make_response
from flask import jsonify
from flask_cors import CORS
from json import JSONEncoder
from collections import OrderedDict
from flask import Blueprint
import psycopg2
import shutil
from pathlib import Path
import os
from sqlalchemy import create_engine
import getpass
from datetime import datetime,date



engine = create_engine('postgresql://postgres:ocpphase01@ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com:5432/offertool')
     

class Database:
    host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com'  # your host
    user='postgres'      # usernames
    password='ocpphase01'
    
    db='offertool'
   
    def __init__(self):
            print('Connection Opened')
            self.connection = psycopg2.connect(dbname=self.db,user=self.user,password=self.password,host=self.host)
           
   
    def insert(self, query):
            print('inside insert')
            var = 'failed'
            try:
                self.cursor = self.connection.cursor()
#                print("HEY")
                var = self.cursor.execute(query)
                print(str(var))
                self.connection.commit()
            except:
                self.connection.rollback()
            finally:
                self.cursor.close()
                print('Cursor closed')
   
            return(var)

    def query(self, query):
        try:
            self.cursor = self.connection.cursor()
            print('inside query')
            self.cursor.execute(query)
            return self.cursor.fetchall()
        finally:
            self.cursor.close()
            print('Cursor closed')
               

db=Database()

input_directory="C:/Users/Administrator/Documents/SMB_INPUT/"

con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')

web_api_response = Blueprint('web_api_response', __name__)

CORS(web_api_response)

@web_api_response.route("/SMBMinibarService",methods=['GET','POST'])
def web_api():
   
         data=json.loads(request.data)
         tableId = data['tableId']
         data=data['data']
         wherestr=''
         db.insert("rollback")
         
         try:
             
             query='select tablename from "SMB"."table_mapping" where id ={}'.format(tableId)
             tableName=db.query(query)[0][0]
            
             k=0
             query='''select * from "SMB"."{}" where '''.format(tableName)
             for i in data:
                 if(k==0):
                     wherestr=wherestr +'"' + i +'"' +" = " + "'" +str(data[i])+"'"
                     k=1
                 if(i=="Currency"):
                      wherestr=wherestr + " and " +'"' + i +'"' +" = " + "''" +str(data[i])+"''"
                      k=1
                    
                 else:
                     wherestr=wherestr + " and " +'"' + i +'"' +" = " + "'" +str(data[i])+"'"
             query=query+wherestr
             print(query)
             data=db.query(query)
                
             df=pd.read_sql(query,con=con)
             df_json=json.loads(df.to_json(orient='records'))
                    
                  
             return {"data":df_json,"status":"sucess"}
         except:
             return {"status":"invalid request"}
               
  