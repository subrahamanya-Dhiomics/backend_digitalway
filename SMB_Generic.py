"""
@author: Sayeesh

"""
# import section (python libraries)

from flask import Blueprint
import pandas as pd
import json
from flask import Flask, request, send_file, render_template, make_response,current_app
from flask import jsonify
from flask_cors import CORS
from json import JSONEncoder
import psycopg2
import shutil
from functools import wraps
from pathlib import Path
import os
from sqlalchemy import create_engine
import getpass
import cryptocode
from datetime import datetime,date
import jwt
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from smb_phase1 import upsert,email



# database connection strings ( ocpphase1 -- > offertool >SMB)

engine = create_engine('postgresql://postgres:ocpphase01@ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com:5432/offertool')
con = psycopg2.connect(dbname='offertool',user='pgadmin',password='Sahara_17',host='offertool2-qa.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')

#database class for updating and fetching the data
class Database:
    host='offertool2-qa.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com'  # your host
    user='pgadmin'      # usernames
    password='Sahara_17'
    db='offertool'
   
   
   
    def __init__(self):
           
            self.connection = psycopg2.connect(dbname=self.db,user=self.user,password=self.password,host=self.host)
             
    def insert(self, query):
           
            var = 'failed'
            try:
                self.cursor = self.connection.cursor()
#                print("HEY")
                var = self.cursor.execute(query)
               
                self.connection.commit()
            except:
                self.connection.rollback()
            finally:
                self.cursor.close()
               
            return(var)

    def query(self, query):
        try:
            self.cursor = self.connection.cursor()
           
            self.cursor.execute(query)
            return self.cursor.fetchall()
        finally:
            self.cursor.close()
 
# File Path 
download_path=input_directory="C:/Users/Administrator/Documents/New 6 SMB Xl-Files/"   
 
# flsk app declaration 
generic = Flask(__name__)
CORS(generic)
db=Database()

# Generic List
@generic.route('/generic_list',methods=['GET','POST'])
def generic_list():
    search_string=request.args.get("search_string")
    tablename = request.args.get("tablename")
    tablename=tablename.replace("*","&")
   
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    if(limit==None):
        limit=500
    if(offset==None):
        offset=0
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    # fetching the data from database and filtering    
    query='''select * from "SMB"."{}" where "active"=1 order by sequence_id OFFSET {} LIMIT {} '''.format(tablename,lowerLimit,upperLimit) 
    df=pd.read_sql(query,con=con )
    count=db.query('select count(*) from "SMB"."{}" where "active"=1 '.format(tablename))[0][0]
    df.columns = df.columns.str.replace(' ', '_')
    
    print("*************** ",query)
    if(search_string!="all" and search_string!=None):
                  df=df[df.eq(search_string).any(1)]
    
    table=json.loads(df.to_json(orient='records'))
    
    return {"data":table,"totalCount":count},200       
  
#  Get Single Data From DB
@generic.route('/generic_get',methods=['GET','POST'])   
def generic_get():
    id_value=request.args.get('id')  
    tablename = request.args.get('tablename')
    tablename = tablename.replace("*","&")
    
    query='''select * from "SMB"."{}" where "id"={} '''.format(tablename,id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        record=json.loads(df.to_json(orient='records'))
        print('recoooooo    ',record)
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
# Add New Data
@generic.route('/generic_add',methods=['POST'])
def generic_add():
    username = getpass.getuser()
    data = json.loads(request.data)
    tablename = data['tablename']
    tablename = tablename.replace("*","&")
    flag='add'
    
    col_df=pd.read_sql(''' select * from "SMB"."{}"'''.format(tablename),con)
    rm_col = ['id','active','aprover1','aprover2','aprover3','updated_on']
    col_df.drop(axis=1,columns=rm_col,inplace=True)
    col_df=list(col_df)
    col_tuple = ["tablename","flag"]
    col_tuple.extend(col_df)
    col_tuple = tuple(col_tuple)
    
    value = data.values()
    value = list(value)
    input_tuple = [tablename,flag,username]
    input_tuple.extend(value)
    input_tuple.pop(-1)
    input_tuple = tuple(input_tuple)
        
    # status=upsert(col_tuple,input_tuple,flag,tablename)
    # if(status['status']=='success'): email_status=email([status['tableid']],tablename)
    
    # if(email_status=='success'): return {"status":"success"},200
    # else: return {"status":"failure"},500
    
    return {'col ':col_tuple,'in ':input_tuple}
    
    
# Delete Data 
@generic.route('/generic_delete',methods=['POST','GET','DELETE'])
def generic_delete():  
    id_value  = json.loads(request.data)['id'][1:]
    tablename = json.loads(request.data)['id'][0]
    tablename = tablename.replace("*","&")
   
    # status=email(id_value,tablename,'delete')
    # if(status=='success'):return {"status":"success"},200
    # else: return {"status":"failure"},500
    
    return {'table ':tablename,'id ':id_value}

# Download Data
@generic.route('/generic_download',methods=['GET'])
def  generic_download():
        tablename = request.args.get('tablename')
        tablename = tablename.replace("*","&")
        now = datetime.now()
        
        col_df=pd.read_sql(''' select * from "SMB"."{}"'''.format(tablename),con)
        rm_col = ['Username','active','aprover1','aprover2','aprover3','updated_on']
        col_df.drop(axis=1,columns=rm_col,inplace=True)
        df=col_df
        
        ls=list(df.columns)
        # ls = ls[-1:] + ls[:-1]
        df=df[ls]
        
        t=now.strftime("%d-%m-%Y-%H-%M-%S")
        file=download_path+ t + tablename + '.xlsx'
        df.to_excel(file,index=False)
        
        return send_file(file, as_attachment=True)
       
# File Upload
@generic.route('/generic_upload', methods=['GET','POST'])
def generic_upload():
        f=request.files['filename']
        f.save(input_directory+f.filename)
        tablename = f.filename
        print(tablename)
        smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
        smb_df.columns = smb_df.columns.str.replace(" ","_")
        smb_df.rename(columns={'Business_Code' :'BusinessCode','Unit_of_Quantity':'UnitOf_Quantity'},inplace=True)
        data = json.loads(smb_df.to_json(orient='records'))
        
        return {"data":data},200

# Update data
@generic.route('/generic_update',methods=['POST'])
def generic_update():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    data = dict(json.loads(request.data))
    tablename=data['tablename'] = data['tablename'].replace("*","&")
    
    col_df=pd.read_sql(''' select * from "SMB"."{}"'''.format(tablename),con)
    rm_col = ['Username','active','aprover1','aprover2','aprover3','updated_on']
    col_df.drop(axis=1,columns=rm_col,inplace=True)
    col_df=list(col_df)
    col_df.append("tablename")
    col_df = col_df[-1:] + col_df[:-1]
    col_tuple = tuple(col_df)
    
    value = list(data.values())
    value = value[-1:] + value[:-1]
    input_tuple = tuple(value)
    
    return {'col_tuple':col_tuple,'in_tuple':input_tuple}
    
generic.run()