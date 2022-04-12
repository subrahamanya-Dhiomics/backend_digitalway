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

from smb_phase1 import upsert,email,tuple_to_string,fun_ignore_sequence_id




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
# download_path=input_directory="C:/Users/Administrator/Documents/"  
 

download_path="/home/ubuntu/mega_dir/"
input_directory="/home/ubuntu/mega_dir/"

# flsk app declaration 
generic = Blueprint('generic', __name__)
CORS(generic)
db=Database()



# Generic List
@generic.route('/generic_list',methods=['GET','POST'])
def generic_list():
    search_string=request.args.get("search_string")
    tablename = request.args.get("table_name")
    
   
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
    print(query)
    df=pd.read_sql(query,con=con )
    count=db.query('select count(*) from "SMB"."{}" where "active"=1 '.format(tablename))[0][0]
    df.columns = df.columns.str.replace(' ', '_')
    
    if(search_string!="all" and search_string!=None):
                  df=df[df.eq(search_string).any(1)]
    
    table=json.loads(df.to_json(orient='records'))
    
    return {"data":table,"totalCount":count},200       
  
#  Get Single Data From DB
@generic.route('/generic_get',methods=['GET','POST'])   
def generic_get():
    id_value=request.args.get('id')  
    tablename = request.args.get('table_name')
   
    
    query='''select * from "SMB"."{}" where "id"={} '''.format(tablename,id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        record=json.loads(df.to_json(orient='records'))
        print('recoooooo    ',record)
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
    
@generic.route('/generic_download',methods=['GET'])
def  generic_download():
        tablename = request.args.get('table_name')
       
        now = datetime.now()
        col_df=pd.read_sql(''' select * from "SMB"."{}" where "active"='1' order by sequence_id  '''.format(tablename),con)
        constant_cols=['id','sequence_id']
        
        rm_col = ['Username','active','aprover1','aprover2','aprover3','updated_on']
        col_df.drop(axis=1,columns=rm_col,inplace=True)
        
        cols=list(col_df.columns)
        for col in constant_cols: cols.remove(col)
        
        col_df=col_df[constant_cols+cols]
        t=now.strftime("%d-%m-%Y-%H-%M-%S")
        file=download_path+ t + tablename + '.xlsx'
        col_df.to_excel(file,index=False)
        
        return send_file(file, as_attachment=True)
    
    
    
    
    
@generic.route('/generic_delete',methods=['POST','GET','DELETE'])
def generic_delete():  
    
    id_value  = json.loads(request.data)['id'][1:]
    username=request.headers['username']
    
    print(id_value)
    print("****")
    tablename = json.loads(request.data)['id'][0]
    
   
   
    status=email(id_value,tablename,'delete',username=username)
    
    if(status=='success'):return {"status":"success"},200
    else: return {"status":"failure"},500
   
    
    
    return {'table ':tablename,'id ':id_value}
    
@generic.route('/generic_add',methods=['POST'])
def generic_add():
    username=request.headers['username']
    
    data = json.loads(request.data)
    tablename = data['table_name']
   
    flag='add'
    del data['table_name']
    
    lis = [n.replace('_', ' ') for n in list(data.keys())]
    col_tuple=tuple(["flag","Username","table_name"]+lis)
    
    input_tuple=tuple([flag,username,tablename]+list(data.values()))
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
    


    
    
    
# File Upload
@generic.route('/generic_upload', methods=['GET','POST'])
def generic_upload():
            f=request.files['filename']
            f.save(input_directory+f.filename)
            tablename = f.filename
            
                
       
            df=pd.read_excel(input_directory+f.filename,dtype=str)
            df['id'] = df['id'].astype(int)
          
            df.drop(axis=1,columns=['sequence_id'],inplace=True)
            
            
            colstr=tuple_to_string(tuple(df.columns))
            
            query=''' select {} from "SMB"."{}" where "active"='1' order by sequence_id  '''.format(colstr,tablename)
            print(query)
            df_main=pd.read_sql(query,con)
            
            try:
             df['Currency'] = df['Currency'].str.replace("'","")
            except:
                pass
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
           
            return {"data":table},200
        
        
@generic.route('/generic_validate',methods=['GET','POST'])
def generic_validate():
    json_data=json.loads(request.data)
    username=request.headers['username']
     
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    print(json_data)
    
    df=pd.DataFrame(json_data["billet"]) 
    
    tablename=json_data['table_name']
    
    
      
    df.insert(0,'Username',username)
    df.insert(1,'table_name',tablename)
    
    
    
    
    df=fun_ignore_sequence_id(df,tablename)        
   
    cols=list(df.columns)
    rm_col=["table_name","id","sequence_id","Username"]
    for c in rm_col:
        cols.remove(c)
    
    df=df[rm_col+cols]
    
    flag='update'  
    
    
    col_tuple=tuple(df.columns)
    id_value=[]
  
  
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
        print("mail_sent")
  
    return {"status":"success"},200
       
            
     
    

@generic.route('/generic_history',methods=['POST','GET'])
def generic_history():
    
    search_string=request.args.get("search_string")
    table_name=request.args.get('table_name')
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
   
    df = pd.read_sql('''select * from "SMB"."{}_History"   OFFSET {} LIMIT {}'''.format(table_name,lowerLimit,upperLimit), con=con)
    count=db.query('select count(*) from "SMB"."{}_History" '.format(table_name))[0][0]
    df.columns = df.columns.str.replace(' ', '_')
    
    df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)
    df['updated_on'] = df['updated_on'].astype('datetime64[s]')
    df['updated_on']=pd.to_datetime(df['updated_on'])
    df['updated_on']=df['updated_on'].astype(str)
   
    # print(df['updated_on'])
    
    if(search_string!="all" and search_string!=None):
                  df=df[df.eq(search_string).any(1)]
    
    table=json.loads(df.to_json(orient='records'))
    
    return {"data":table,"totalCount":count},200         
   
    



    

# Update data

@generic.route('/generic_update',methods=['POST'])
def generic_update():
    
    today = date.today()
    username=request.headers['username']
    
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    
    data = dict(json.loads(request.data))

    tablename=data['table_name']
    id_value=data['id_value']
    flag='update'
    df=pd.DataFrame([data])
  
    rm_col=["table_name","id_value","sequence_id","Username"]
    for k in rm_col:
       del data[k]
    
    df=df[rm_col+list(data.keys())]
    
    db_cols=[n.replace('_', ' ') for n in list(data.keys())]
    col_tuple=["table_name","id","sequence_id","Username"] + db_cols
    input_tuple=tuple(df.loc[0])
    
    
    print(col_tuple)
    print(input_tuple)
    print("*****")
    
    email_status=''
    
    status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
    if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)
    
    
    
    if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
    else: return {"status":"failure"},500




