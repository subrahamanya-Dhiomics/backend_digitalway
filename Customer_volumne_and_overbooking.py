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

@app.route("/customer_volume_and_overbooking",methods=['GET','POST'])
def customer_volume_and_overbooking():
    search_string=request.args.get("search_string")
    
    customer=request.args.get("Customer")
   
    
    
    
    wherestr=''
  
    if(customer!='all' and customer!=None):
        wherestr+='where customer = {} '.format(customer)
       
      
            
      
        
    query='''SELECT plant,division,matgrp,gradeinternal,billetsourcing,sequencegraduation from offertool.basecost where division='SEM';'''.format(wherestr)

    df = pd.read_sql(query, con=con)
    
    data=json.loads(df.to_json(orient='records'))
    
    customer_name=list(set(df.accountname))

    
    
    
    
    
    
    return {"customer_name":customer_name},200

        



if __name__ == '__main__':
    app.run()