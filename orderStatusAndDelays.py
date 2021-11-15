# -*- coding: utf-8 -*-
"""
Created on Sat Nov 13 13:21:31 2021

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


#download_path='C:/SMB/smb_download/'

con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')

app=Flask(__name__)
CORS(app)

@app.route("/order_statusAnDdelays",methods=['GET','POST'])
def orderStatusAndDelays():
    search_string=request.args.get("search_string")
    
    status_text=request.args.get("order_status")
    mat_range=request.args.get("sold_to")
    mat_div=request.args.get("ship_to")
    mat=request.args.get("sales_doc_number",type=int)
    mill=request.args.get("DELV_WEEK")
    
    
    
    wherestr=''
    flag=0
    
    
    if(status_text!='all' and status_text!=None):
       
        if(flag==0):
            wherestr+='where status_text = {} '.format(status_text)
        else:
            wherestr+=' and  status_text ={}'.format(status_text)
        flag=1
    if(mat_range!='all' and mat_range!=None ):
       
        if(flag==0):
            wherestr+='where mat_range = {}'.format(mat_range)
        else:
            wherestr+=' and  mat_range ={}'.format(mat_range)
        flag=1
        
    if(mat_div!='all' and mat_div!=None):
        
        if(flag==0):
            wherestr+='where mat_div = {}'.format(mat_div)
        else:
            wherestr+=' and  mat_div = {}'.format(mat_div)
        flag=1
    
    if(mat!='all' and mat!=None ):
        
        if(flag==0):
            wherestr+='where mat = {}'.format(mat)
        else:
            wherestr+=' and mat = {}'.format(mat)
        flag=1
    
    if(mill !='all' and mill != None ):
        
        if(flag==0):
            wherestr+='where  size = {}'.format(mill)
        else:
            wherestr+=' and size = {}'.format(mill)
        flag=1



    

    query='''select mat,mill,status_text,date_min,mill_dl,mat_range,mat_div,size from offertool.rolling {}'''.format(wherestr)
    
    print(query)
        
    df = pd.read_sql(query, con=con)
    
    if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
    
    
    data=json.loads(df.to_json(orient='records'))
    
    order_status=list(set(df.status_text))
    Sold_to=list(set(df.mat_range))
    Ship_to=list(set(df.mat_div))
    DELV_WEEK = list(set(df.mill))
    
    
    
    

    return{"data":data,"order_status":order_status,"Sold_to":Sold_to,"Ship_to":Ship_to,"DELV_WEEK":DELV_WEEK}


if __name__ == '__main__':
    app.run()