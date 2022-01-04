# -*- coding: utf-8 -*-
"""
Created on Tue Jan  4 03:56:26 2022

@author: Administrator
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
import json
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


#taskbar1 = Blueprint('taskbar1', __name__)
app=Flask(__name__)
CORS(app)

con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')

cur = con.cursor()
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



@app.route('/order_status_delay',methods=['POST','GET'])
def order_status_delay():
    search_string=request.args.get('search_string')
    
    wherestr=''
    query='''select A.VBELN as sales_doc_number,
A.POSNR as sales_doc_item_number ,
null as order_status,
A.ZZLIEFDAT_01 as confirmed_delivery_date,  
A.WERKS as delivering_plant, 
C.KUNNR as sold_to, 
C.KUNNR as ship_to, 
A.KWMENG as quantity,
B.BSTNK as customer_reference
from invoice.vbap A 
inner join invoice.vbak B on (B.VBELN = A.VBELN) 
inner join  invoice.ocp_vbpa C on ((C.VBELN = A.VBELN) and (C.POSNR = A.POSNR) and (C.PARVW = 'AG')
      or (C.VBELN = A.VBELN) and (C.POSNR = 'Initial') and (C.PARVW = 'AG') 
	  or (C.VBELN = A.VBELN) and (C.PARVW = 'AG')) or ((C.VBELN = A.VBELN) and (C.POSNR = A.POSNR) and (C.PARVW = 'WE')
      or (C.VBELN = A.VBELN) and (C.POSNR = 'Initial') and (C.PARVW = 'WE') 
	  or (C.VBELN = A.VBELN) and (C.PARVW = 'WE'))'''
    
             

    df=pd.read_sql(query,con=con)
    
    if(search_string!="All" and search_string!='all' and search_string!=None):
                              df=df[df.eq(search_string).any(1)]
                              
    df_json=json.loads(df.to_json(orient='records'))
    
    
    order_status=list(set(df.order_status))
    sold_to=list(set(df.sold_to))
    ship_to=list(set(df.ship_to))
    
    return {"data":df_json,"oder_status":order_status,"sold_to":sold_to,"ship_to":ship_to },200


if __name__=="__main__":
    app.run()