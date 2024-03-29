# -*- coding: utf-8 -*-
"""
Created on Wed Oct 20 10:07:00 2021

@author: Administrator
"""

from flask import Blueprint,current_app
import pandas as pd
import time
import json
from flask import Flask, request, send_file, render_template
from flask import jsonify
from flask_cors import CORS
from json import JSONEncoder
from functools import wraps
from collections import OrderedDict
from flask import Blueprint


from smb_phase1 import fun_ignore_sequence_id

import psycopg2

import shutil
from pathlib import Path
import os
from sqlalchemy import create_engine
import getpass
from datetime import datetime,date

from smb_phase1 import token_required
from smb_phase1 import upsert
from smb_phase1 import email

from smb_phase1 import Database



from smb_phase1 import highlight_col

# engine = create_engine('postgresql://postgres:ocpphase01@ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com:5432/offertool')
     

               
 
smb_app2 = Blueprint('smb_app2', __name__)

CORS(smb_app2)


# download_path=input_directory="C:/Users/Administrator/Documents/"
download_path=input_directory="/home/ubuntu/mega_dir/"


db=Database()
# Login page


# db connection opening and closing before and after the every api calls 

@smb_app2.before_request
def before_request():
   
    current_app.config['CON']  = psycopg2.connect(dbname='offertool', user='pgadmin', password='Sahara_17',
                       host='offertool2-qa.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')
    

@smb_app2.after_request
def after_request(response):
     current_app.config['CON'].close()
     return response
 

# *****************************************************************************************************************************************
# freight_parity

@smb_app2.route('/data_freight_parity',methods=['GET','POST'])
@token_required
def  freight_parity():
    # query_paramters 
    search_string=request.args.get("search_string")
    
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
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Freight Parity" where "active"='1' order by sequence_id  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=current_app.config['CON'])
        count=db.query('select count(*) from "SMB"."SMB - Extra - Freight Parity" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Zip_Code_(Dest)":"Zip_Code_Dest"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app2.route('/delete_record_freight_parity',methods=['POST','GET','DELETE'])
@token_required
def delete_record_delivery_mill_minibar():  
    
   
    id_value,username=json.loads(request.data)['id'],request.headers['username']
    
    
    tablename='SMB - Extra - Freight Parity'
   
    status=email(id_value,tablename,'delete',username=username)
    if(status=='success'):return {"status":"success"},200
    else: return {"status":"failure"},500



@smb_app2.route('/get_record_freight_parity',methods=['GET','POST'])    
@token_required   
def get_record_freight_parity():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Freight Parity" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=current_app.config['CON'])
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Zip Code (Dest)":"Zip_Code_Dest"},inplace=True)
        
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app2.route('/update_record_freight_parity',methods=['POST'])
@token_required
def update_record_frieght_parity():
    
    today = date.today()
    username=request.headers['username']
       
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    Market_Country=(query_parameters['Market_Country'])
    Zip_Code_Dest=(query_parameters['Zip_Code_Dest'])
    
    Product_Division =( query_parameters["Product_Division"])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    id_value=(query_parameters['id_value'])
    sequence_id=(query_parameters['sequence_id'])
    
    flag='update'
      
    tablename='SMB - Extra - Freight Parity'        
   
    
    input_tuple=(tablename,id_value,sequence_id,username,Delivering_Mill,Market_Country,Zip_Code_Dest,Product_Division,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name","id","sequence_id","Username",
    "Delivering Mill",
    "Market - Country",
    "Zip Code (Dest)",
    "Product Division",
    "Document Item Currency",
    "Amount",
    "Currency")
    email_status=''
    
    status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
    if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)
    
    
    
    if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
    else: return {"status":"failure"},500


     
@smb_app2.route('/add_record_freight_parity',methods=['POST'])
@token_required
def add_record_frieght_parity():
    
    
    username=request.headers['username']
       
    
    query_parameters =json.loads(request.data)
    
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    Market_Country=(query_parameters['Market_Country'])
    Zip_Code_Dest=(query_parameters['Zip_Code_Dest'])
    
    Product_Division =( query_parameters["Product_Division"])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
   
    
    flag='add'
        
    tablename='SMB - Extra - Freight Parity'        
   
    
    input_tuple=(tablename,flag,username,Delivering_Mill,Market_Country,Zip_Code_Dest,Product_Division,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name","flag", "Username",
           "Delivering Mill",
    "Market - Country",
    "Zip Code (Dest)",
    "Product Division",
    "Document Item Currency",
    "Amount",
    "Currency")
    
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
    


    
   
@smb_app2.route('/upload_freight_parity', methods=['GET','POST'])
@token_required
def upload_freight_parity():
    
            f=request.files['filename']
      
                
            f.save(input_directory+f.filename)
            
       
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","Delivering Mill", "Market - Country",
       "Zip Code (Dest)", "Product Division", "Document Item Currency",
       "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            
            df_main = pd.read_sql('''select "id","Delivering Mill", "Market - Country",
       "Zip Code (Dest)", "Product Division", "Document Item Currency",
       "Amount", "Currency" from "SMB"."SMB - Extra - Freight Parity" where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
            df['Currency'] = df['Currency'].str.replace("'","")
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country","Zip_Code_(Dest)":"Zip_Code_Dest"},inplace=True)
        
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
       

@smb_app2.route('/validate_freight_parity', methods=['GET','POST'])
@token_required
def  validate_freight_parity():
    
        
    json_data=json.loads(request.data)
    username=request.headers['username']
        
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    
             
       
    tablename='SMB - Extra - Freight Parity'
    df=fun_ignore_sequence_id(df,tablename)        
   
    
    flag='update'  
    
    df.insert(1,'table_name',tablename)
    
    
    col_tuple=("table_name","id","sequence_id",
          "Username","Delivering Mill",
    "Market - Country",
    "Zip Code (Dest)",
    "Product Division",
    "Document Item Currency",
    "Amount",
    "Currency")
    col_list=['table_name','id','sequence_id','Username','Delivering_Mill','Market_Country','Zip_Code_Dest','Product_Division','Document_Item_Currency','Amount','Currency']
    
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
        print("mail_sent")
  
    return {"status":"success"},200
         
    
    
@smb_app2.route('/download_freight_parity',methods=['GET'])

def download_freight_parity():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select  "id",sequence_id,"Delivering Mill","Market - Country","Zip Code (Dest)","Product Division","Document Item Currency","Amount", "Currency" from "SMB"."SMB - Extra - Freight Parity" where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'freight_parity.xlsx'
            df=df.style.apply(highlight_col, axis=None)
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
           
        except:
            return {"status":"failure"},500


# *****************************************************************************************************************************************
# freight_parity_minibar


@smb_app2.route('/data_freight_parity_minibar',methods=['GET','POST'])
@token_required
def  freight_parity_minibar():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    
    if(limit==None):
        limit=500
    if(offset==None):
        offset=0
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    try:
        search_string=int(search_string)
    except:
        search_string=search_string   
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Freight Parity - MiniBar" where "active"='1' order by sequence_id  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=current_app.config['CON'])
        count=db.query('select count(*) from "SMB"."SMB - Extra - Freight Parity - MiniBar" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer","Zip_Code_(Dest)":"Zip_Code_Dest"},inplace=True)  
                
            
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app2.route('/delete_record_freight_parity_minibar',methods=['POST','GET','DELETE'])
@token_required
def delete_record_freight_parity_minibar():  
   
    
    id_value,username=json.loads(request.data)['id'],request.headers['username']
    
    
    tablename='SMB - Extra - Freight Parity - MiniBar'
   
    status=email(id_value,tablename,'delete',username=username)
    if(status=='success'):return {"status":"success"},200
    else: return {"status":"failure"},500


@smb_app2.route('/get_record_freight_parity_minibar',methods=['GET','POST'])       
def get_record_freight_parity_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Freight Parity - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=current_app.config['CON'])
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer","Zip_Code_(Dest)":"Zip_Code_Dest"},inplace=True)  
           
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app2.route('/update_record_freight_parity_minibar',methods=['POST'])
@token_required
def update_record_frieght_parity_minibar():
    
    today = date.today()
    
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    username=request.headers['username']
       
    query_parameters =json.loads(request.data)
    
    sequence_id=(query_parameters['sequence_id'])
    
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    Market_Country=(query_parameters['Market_Country'])
    Market_Customer_Group=(query_parameters['Market_Customer_Group'])
    Market_Customer=(query_parameters['Market_Customer'])
    Zip_Code_Dest=(query_parameters['Zip_Code_Dest'])
    
    Product_Division =( query_parameters["Product_Division"])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    id_value=(query_parameters['id_value'])
    
    flag='update'
  
    tablename='SMB - Extra - Freight Parity - MiniBar'        
    email_status=''
    
    input_tuple=(tablename,id_value,sequence_id,username,Delivering_Mill,Market_Country,Market_Customer_Group,Market_Customer,Zip_Code_Dest,Product_Division,Document_Item_Currency,Amount,Currency)	
    col_tuple=("table_name","id","sequence_id","Username","Delivering Mill","Market - Country","Market - Customer Group","Market - Customer","Zip Code (Dest)","Product Division",
"Document Item Currency","Amount","Currency")
    
    status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
    if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)
    
    
    
    if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
    else: return {"status":"failure"},500




@smb_app2.route('/add_record_freight_parity_minibar',methods=['POST'])
@token_required
def add_record_frieght_parity_minibar():
    username=request.headers['username']
       
    query_parameters =json.loads(request.data)
    Market_Customer=(query_parameters['Market_Customer'])
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    Market_Country=(query_parameters['Market_Country'])
    Market_Customer_Group=(query_parameters['Market_Customer_Group'])
    
   
    Zip_Code_Dest=(query_parameters['Zip_Code_Dest'])
    
    Product_Division =( query_parameters["Product_Division"])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
        
    flag='add'
    
    
    tablename='SMB - Extra - Freight Parity - MiniBar'        
    
    # id_value=db.query('''select "SMB".get_max_id('"SMB"."SMB - Base Price - Category Addition"')''')[0][0]+1
    
    
    
    input_tuple=(tablename,flag,username,Delivering_Mill,Market_Country,Market_Customer_Group,Market_Customer,Zip_Code_Dest,Product_Division,Document_Item_Currency,Amount,Currency)	
    col_tuple=("table_name","flag", "Username",
             "Delivering Mill","Market - Country","Market - Customer Group","Market - Customer","Zip Code (Dest)","Product Division",
"Document Item Currency","Amount","Currency")
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
            
  

   
@smb_app2.route('/upload_freight_parity_minibar', methods=['GET','POST'])
@token_required
def upload_freight_parity_minibar():
    
            f=request.files['filename']
      
                
            f.save(input_directory+f.filename)
            
       
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","Delivering Mill", "Market - Country",
       "Market - Customer Group", "Market - Customer","Zip Code (Dest)",
       "Product Division", "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            df_main = pd.read_sql('''select "id","Delivering Mill", "Market - Country",
       "Market - Customer Group","Market - Customer","Zip Code (Dest)",
       "Product Division", "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Freight Parity - MiniBar" where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
            
            df['Currency'] = df['Currency'].str.replace("'","")
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer","Zip_Code_(Dest)":"Zip_Code_Dest"},inplace=True)  
               
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
     

@smb_app2.route('/validate_freight_parity_minibar', methods=['GET','POST'])
@token_required
def  validate_freight_parity_minibar():
    
        
    json_data=json.loads(request.data)
    username=request.headers['username']
        
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    email_status=''
    df.insert(0,'Username',username)
    
    tablename='SMB - Extra - Freight Parity - MiniBar'    
    df=fun_ignore_sequence_id(df,tablename)    
     
    flag='update'  
    df.insert(1,'table_name',tablename)
    col_tuple=("table_name","id","sequence_id",
             "Username",
              "Delivering Mill","Market - Country","Market - Customer Group","Market - Customer","Zip Code (Dest)","Product Division",
"Document Item Currency","Amount","Currency")
    col_list=['table_name','id','sequence_id','Username','Delivering_Mill','Market_Country','Market_Customer_Group','Market_Customer','Zip_Code_Dest','Product_Division','Document_Item_Currency','Amount','Currency']
    
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
  
    return {"status":"success"},200
      
         
@smb_app2.route('/download_freight_parity_minibar',methods=['GET'])

def download_freight_parity_minibar():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select id,sequence_id,"Delivering Mill","Market - Country","Market - Customer Group","Market - Customer","Zip Code (Dest)","Product Division","Document Item Currency","Amount", "Currency" from "SMB"."SMB - Extra - Freight Parity - MiniBar"  where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'freight_parity_minibar.xlsx'
            df=df.style.apply(highlight_col, axis=None)
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
        except:
            return {"status":"failure"},500



# *****************************************************************************************************************************************
# "SMB"."SMB - Extra - Grade"



@smb_app2.route('/data_extra_grade',methods=['GET','POST'])

def  extra_grade_data():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Grade" where "active"='1' order by sequence_id  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=current_app.config['CON'])
        count=db.query('select count(*) from "SMB"."SMB - Extra - Grade" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
                
            
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app2.route('/delete_record_extra_grade',methods=['POST','GET','DELETE'])
@token_required
def delete_record_extra_grade():  
    
    id_value,username=json.loads(request.data)['id'],request.headers['username']
    
    
    tablename='SMB - Extra - Grade'
   
    status=email(id_value,tablename,'delete',username=username)
    if(status=='success'):return {"status":"success"},200
    else: return {"status":"failure"},500


@smb_app2.route('/get_record_extra_grade',methods=['GET','POST'])  
@token_required     
def get_record_extra_grade():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Grade" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=current_app.config['CON'])
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
    
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app2.route('/update_record_extra_grade',methods=['POST'])
@token_required
def update_record_extra_grade():
    
    today = date.today()
    username=request.headers['username']
       
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    username=request.headers['username']
       
    
    
    BusinessCode=(query_parameters["BusinessCode"])
    Grade_Category=(query_parameters["Grade_Category"])
   
    Market_Country=(query_parameters['Market_Country'])
    sequence_id=(query_parameters['sequence_id'])
    
    id_value=(query_parameters['id_value'])
    Product_Division =( query_parameters["Product_Division"])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])

    flag='update'
  
    tablename='SMB - Extra - Grade'
    input_tuple=(tablename,id_value,sequence_id,username,BusinessCode,Grade_Category,Market_Country,Document_Item_Currency,Product_Division,Amount,Currency)
    col_tuple=("table_name","id","sequence_id","Username",
          "BusinessCode",
        "Grade Category",
        "Market - Country",
        "Document Item Currency",
        "Product Division",
       
        "Amount",
        "Currency")
    email_status=''
    
    status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
    if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)
    
    
    
    if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
    else: return {"status":"failure"},500



    
   
@smb_app2.route('/add_record_extra_grade',methods=['POST'])
@token_required
def add_record_extra_grade():
     
    query_parameters =json.loads(request.data)
    username=request.headers['username']
       
    
    
    BusinessCode=(query_parameters["BusinessCode"])
    Grade_Category=(query_parameters["Grade_Category"])
   
    Market_Country=(query_parameters['Market_Country'])
    
   
    Product_Division =( query_parameters["Product_Division"])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    
    flag='add'
  
    tablename='SMB - Extra - Grade'        
    
    input_tuple=(tablename,flag,username,BusinessCode,Grade_Category,Market_Country,Document_Item_Currency,Product_Division,Amount,Currency)
    col_tuple=("table_name","flag", "Username", "BusinessCode",
"Grade Category",
"Market - Country",
"Document Item Currency",
"Product Division",

"Amount",
"Currency")
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
       
# **

@smb_app2.route('/upload_extra_grade', methods=['GET','POST'])
@token_required
def upload_extra_grade():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","BusinessCode", "Grade Category",
        "Market - Country", "Product Division",
       "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
           
            df['Currency'] = df['Currency'].str.replace("'","")
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Grade Category",
       "Market - Country", "Product Division",
       "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Grade" where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500

@smb_app2.route('/validate_extra_grade', methods=['GET','POST'])
@token_required
def  validate_extra_grade():      
    json_data=json.loads(request.data)
    username=request.headers['username']
        
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    email_status=''
    df.insert(0,'Username',username)
    
    tablename='SMB - Extra - Grade'  
    df=fun_ignore_sequence_id(df,tablename)
    flag='update'  
    df.insert(1,'table_name',tablename)
    col_tuple=("table_name","id","sequence_id",
             "Username",
            "BusinessCode",
            "Grade Category",
            "Market - Country",
            "Document Item Currency",
            "Product Division",
           
            "Amount",
            "Currency")
    col_list=['table_name','id','sequence_id','Username','BusinessCode','Grade_Category','Market_Country','Document_Item_Currency','Product_Division','Amount','Currency']
    
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
  
    return {"status":"success"},200
         
@smb_app2.route('/download_extra_grade',methods=['GET'])

def download_extra_grade():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select "id",sequence_id,"BusinessCode", "Grade Category",
       "Market - Country", "Product Division",
       "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Grade" where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'extra_grade.xlsx'
            df=df.style.apply(highlight_col, axis=None)
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
        except:
            return {"status":"failure"},500

# ***********************************************************************************************************************************************************************

# ***********************************************************************************************************************************************************************
# "SMB"."SMB - Extra - Grade - MiniBar"



@smb_app2.route('/data_extra_grade_minibar',methods=['GET','POST'])
@token_required
def  extra_grade_data_minibar():
    # query_paramters 
    search_string=request.args.get("search_string")
    
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
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Grade - MiniBar" where "active"='1' order by sequence_id  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=current_app.config['CON'])
        
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)  
        count=db.query('select count(*) from "SMB"."SMB - Extra - Grade - MiniBar" where "active"=1 ')[0][0]
            
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        
@smb_app2.route('/delete_record_extra_grade_minibar',methods=['POST','GET','DELETE'])
@token_required
def delete_record_extra_grade_minibar():  
    
    id_value,username=json.loads(request.data)['id'],request.headers['username']
    
    
    tablename='SMB - Extra - Grade - MiniBar'
   
    status=email(id_value,tablename,'delete',username=username)
    if(status=='success'):return {"status":"success"},200
    else: return {"status":"failure"},500



@smb_app2.route('/get_record_extra_grade_minibar',methods=['GET','POST'])  
@token_required     
def get_record_extra_grade_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Grade - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=current_app.config['CON'])
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)  
         
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app2.route('/add_record_extra_grade_minibar',methods=['POST'])
@token_required

def add_record_extra_grade_minibar():
    
    today = date.today()
    username=request.headers['username']
       
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
        
    BusinessCode=(query_parameters["BusinessCode"])
    Customer_Group=(query_parameters["Customer_Group"]) 
    Market_Country=(query_parameters['Market_Country'])
    Market_Customer=(query_parameters['Market_Customer'])
    Grade_Category=(query_parameters['Grade_Category'])
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    # try:
    #     input_tuple=( username,BusinessCode,Customer_Group,Market_Country,Grade_Category ,Document_Item_Currency, Amount, Currency.strip("'"))
        
    #     query='''INSERT INTO "SMB"."SMB - Extra - Grade - MiniBar"(
             
    #          "Username",
             
    #          "BusinessCode",
    #          "Customer Group",
          
    #        "Market - Country",
    #        "Grade Category",
    #          "Document Item Currency",
    #          "Amount",
    #          "Currency")
    #          VALUES{};'''.format(input_tuple)
         
       
    #     result=db.insert(query)
    #     if result=='failed': raise ValueError
    #     return {"status":"success"},200
    # except:
    #     return {"status":"failure"},500
    flag='add'
   
    tablename='SMB - Extra - Grade - MiniBar'
   
    input_tuple=(tablename,flag,username, BusinessCode,Customer_Group,Market_Customer,Market_Country,Grade_Category,Document_Item_Currency, Amount,Currency.strip("'"))
    col_tuple=("table_name",
               "flag",  
               "Username",             
             "BusinessCode",
             "Customer Group", 
             "Market - Customer",
             "Market - Country",
            	 "Grade Category",
             "Document Item Currency",
             "Amount",
             "Currency")
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
    
    

@smb_app2.route('/update_record_extra_grade_minibar',methods=['POST'])
@token_required
def update_record_extra_grade_minibar():
    
    today = date.today()
    username=request.headers['username']
       
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    sequence_id=(query_parameters['sequence_id'])
    Market_Customer=(query_parameters['Market_Customer'])
    
    
    BusinessCode=(query_parameters["BusinessCode"])
    Customer_Group=(query_parameters["Customer_Group"])
    
    
    Market_Country=(query_parameters['Market_Country'])
    
    Grade_Category=(query_parameters['Grade_Category'])
    id_value=(query_parameters['id_value'])
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    # try:
        
    #     query1='''INSERT INTO "SMB"."SMB - Extra - Grade - MiniBar_History"
    #     SELECT 
        # "id","Username",now(),"BusinessCode", "Customer Group",
        # "Market - Customer", "Market - Country", "Grade Category",
        # "Document Item Currency", "Amount", "Currency" 
        # FROM "SMB"."SMB - Extra - Grade - MiniBar"
    #     WHERE "id"={} '''.format(id_value)
    #     result=db.insert(query1)
    #     if result=='failed' :raise ValueError
    
    #     query2='''UPDATE "SMB"."SMB - Extra - Grade - MiniBar"
    #     SET 
    #    "Username"='{0}',
    #    "BusinessCode"='{1}', "Customer Group"='{2}',
    #    "Market - Country"='{3}', "Grade Category"='{4}',
       
       
    #    "Document Item Currency"='{5}',
    #    "Amount"='{6}',
    #    "Currency"=''{7}'',
    #    "updated_on"='{8}',sequence_id= {9}
    #     WHERE "id"={10} '''.format(username,BusinessCode,Customer_Group,Market_Country,Grade_Category,Document_Item_Currency,Amount,Currency,date_time,sequence_id,id_value)
    #     result1=db.insert(query2)
    #     if result1=='failed' :raise ValueError
    #     print(query1)
    #     print("*****************************")
    #     print(query2)
    #     return {"status":"success"},200
    # except:
    #     return {"status":"failure"},500
    flag='update'
      
    tablename='SMB - Extra - Grade - MiniBar'     
    
    input_tuple=(tablename,id_value,sequence_id,username,BusinessCode,Customer_Group,Market_Customer,Market_Country,Grade_Category,Document_Item_Currency, Amount,Currency)

    col_tuple=("table_name",
               "id",
               "sequence_id",
              "Username",
              "BusinessCode",
              "Customer Group",
              "Market - Customer",
            
              "Market - Country",
              "Grade Category",
              "Document Item Currency",
              "Amount",
              "Currency")
    email_status=''
    
    status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
    if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)      
        
    
    if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
    else: return {"status":"failure"},500


    
   
@smb_app2.route('/upload_extra_grade_minibar', methods=['GET','POST'])
@token_required
def upload_extra_grade_minibar():
    
         f=request.files['filename']
      
                
         f.save(input_directory+f.filename)
         
         
         smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
         
         df=smb_df[["id","BusinessCode", "Customer Group","Market - Customer",
      "Market - Country", "Grade Category",
       "Document Item Currency", "Amount", "Currency"]]  
         df["id"]=df["id"].astype(int)
         
         
         df_main = pd.read_sql('''select "id","BusinessCode", "Customer Group","Market - Customer",
       "Market - Country", "Grade Category",
       "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Grade - MiniBar" where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
         
         df['Currency'] = df['Currency'].str.replace("'","")
         df3 = df.merge(df_main, how='left', indicator=True)
         df3=df3[df3['_merge']=='left_only']
         
         df3.columns = df3.columns.str.replace(' ', '_')
         df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
            
         df3.drop(['_merge'],axis=1,inplace=True)
         
         table=json.loads(df3.to_json(orient='records'))
         
         return {"data":table},200
   


@smb_app2.route('/validate_extra_grade_minibar', methods=['GET','POST'])
@token_required
def  validate_extra_grade_minibar():
    
        
    json_data=json.loads(request.data)
    username=request.headers['username']
        
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    df.insert(1,'date_time',date_time)
    # try:
       
    #     df=df[ ["Username","BusinessCode", "Customer_Group",
    #   "Market_Country", "Grade_Category",
    #    "Document_Item_Currency", "Amount", "Currency","date_time","sequence_id","id"]]
        
    #     query1='''INSERT INTO "SMB"."SMB - Extra - Grade - MiniBar_History" 
    #     SELECT 
    #     "id",
    #     "Username",now(),
    #     "BusinessCode", "Customer Group",
    #    "Market - Customer", "Market - Country", "Grade Category",
    #    "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Grade - MiniBar"
    #     WHERE "id" in {} '''
        
    #     id_tuple=tuple(df["id"])
    #     if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
                    
    #     result=db.insert(query1.format(id_tuple))
    #     if result=='failed' :raise ValueError
        
        
    #     # looping for update query
    #     for i in range(0,len(df)):
    #         print(tuple(df.loc[0]))
    #         query2='''UPDATE "SMB"."SMB - Extra - Grade - MiniBar"
    #     SET 
    #    "Username"='%s',
    #    "BusinessCode"='%s', "Customer Group"='%s',
    #     "Market - Country"='%s', "Grade Category"='%s',
       
    #    "Document Item Currency"='%s',
    #    "Amount"='%s',
    #    "Currency"=''%s'',
    #    "updated_on"='%s',sequence_id='%s'
    #     WHERE "id"= '%s' ''' % tuple(df.loc[i])
    #         result=db.insert(query2)
    #         if result=='failed' :raise ValueError
            
    #     return {"status":"success"},200
    # except:
    #     return {"status":"failure"},500
    tablename='SMB - Extra - Grade - MiniBar'
    df=fun_ignore_sequence_id(df,tablename)
    flag='update'  
    
    df.insert(1,'table_name',tablename)
    
    
    col_tuple=("table_name",
               "sequence_id",
               "id",
    "Username",
    "BusinessCode", 
    "Customer Group",
    "Market - Customer",
     
    "Market - Country", 
    "Grade Category",
    "Document Item Currency", 
    "Amount", 
    "Currency")
    col_list=["table_name","sequence_id",
               "id",
    "Username",
    "BusinessCode", 
    "Customer_Group",
    "Market_Customer",
    "Market_Country", 
    "Grade_Category",
    "Document_Item_Currency", 
    "Amount", 
    "Currency"]
    
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
  
    return {"status":"success"},200
        
         
@smb_app2.route('/download_extra_grade_minibar',methods=['GET'])

def download_extra_grade_minibar():
        now = datetime.now()
        try:
            df = pd.read_sql('''select id,sequence_id,"BusinessCode","Customer Group","Market - Customer","Market - Country","Grade Category","Document Item Currency","Amount", "Currency" from "SMB"."SMB - Extra - Grade - MiniBar" where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'extra_grade_minibar.xlsx'
            df=df.style.apply(highlight_col, axis=None)
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
        except:
            return {"status":"failure"},500

   



# ***********************************************************************************************************************************************************************
# "SMB"."SMB - Extra - Profile"

@smb_app2.route('/data_extra_profile',methods=['GET','POST'])
@token_required
def  extra_profile():
    # query_paramters 
    search_string=request.args.get("search_string")
    
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
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Profile" where "active"='1' order by sequence_id  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=current_app.config['CON'])
        count=db.query('select count(*) from "SMB"."SMB - Extra - Profile" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
                
            
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app2.route('/delete_record_extra_profile',methods=['POST','GET','DELETE'])
@token_required
def delete_record_extra_profile():  
    
    id_value,username=json.loads(request.data)['id'],request.headers['username']
    
    
    tablename='SMB - Extra - Profile'
   
    status=email(id_value,tablename,'delete',username=username)
    if(status=='success'):return {"status":"success"},200
    else: return {"status":"failure"},500


@smb_app2.route('/get_record_extra_profile',methods=['GET','POST'])   
@token_required    
def get_record_extra_profile():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Profile" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=current_app.config['CON'])
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
         
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app2.route('/add_record_extra_profile',methods=['POST'])
@token_required
def add_record_extra_profile():
    
    
    username=request.headers['username']
       
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters["BusinessCode"])
    Market_Country=(query_parameters['Market_Country'])
    Product_Division=(query_parameters['Product_Division'])
    Product_Level_04=(query_parameters['Product_Level_04'])
    Product_Level_05=(query_parameters['Product_Level_05'])
    Product_Level_02=(query_parameters['Product_Level_02'])
    Delivering_Mill=(query_parameters['Delivering_Mill'])
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    # try:
    #     input_tuple=( username,BusinessCode,Market_Country,Product_Division,Product_Level_04,Product_Level_05,Product_Level_02,Delivering_Mill,Document_Item_Currency, Amount, Currency.strip("'"))
        
    #     query='''INSERT INTO "SMB"."SMB - Extra - Profile"(
             
    #          "Username",
             
        #       "BusinessCode",
        #       "Market - Country",
        # "Product Division",
        # "Product Level 04", 
        # "Product Level 05",
        # "Product Level 02", 
        # "Delivering Mill",
        #       "Document Item Currency",
        #       "Amount",
        #       "Currency")
    #          VALUES{};'''.format(input_tuple)
         
       
    #     result=db.insert(query)
    #     if result=='failed' :raise ValueError
    #     return {"status":"success"},200
    # except:
    #     return {"status":"failure"},500
    flag='add'   
    tablename='SMB - Extra - Profile'
    input_tuple=(tablename,flag,username, BusinessCode,Market_Country,Product_Division,Product_Level_04,Product_Level_05,Product_Level_02,Delivering_Mill,Document_Item_Currency, Amount,Currency.strip("'"))
    col_tuple=("table_name",
               "flag",  
               "Username",             
               "BusinessCode",
              "Market - Country",
                "Product Division",
                "Product Level 04", 
                "Product Level 05",
                "Product Level 02", 
                "Delivering Mill",
              "Document Item Currency",
              "Amount",
              "Currency")
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
    

@smb_app2.route('/update_record_extra_profile',methods=['POST'])
@token_required
def update_record_extra_profile():
    
    today = date.today()
    username=request.headers['username']
       
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    
    BusinessCode=(query_parameters["BusinessCode"])
    Market_Country=(query_parameters['Market_Country'])
    Product_Division=(query_parameters['Product_Division'])
    Product_Level_04=(query_parameters['Product_Level_04'])
    Product_Level_05=(query_parameters['Product_Level_05'])
    Product_Level_02=(query_parameters['Product_Level_02'])
    Delivering_Mill=(query_parameters['Delivering_Mill'])
    id_value=(query_parameters['id_value'])
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    sequence_id=(query_parameters["sequence_id"])
    flag='update'
      
    tablename='SMB - Extra - Profile'   
    
    input_tuple=(tablename,id_value,sequence_id,username,BusinessCode,Market_Country,Product_Division,Product_Level_04,Product_Level_05,Product_Level_02,Delivering_Mill,Document_Item_Currency, Amount,Currency)

    col_tuple=("table_name",
               "id",
               "sequence_id",
              "Username",
              "BusinessCode",
              "Market - Country",
                "Product Division",
                "Product Level 04", 
                "Product Level 05",
                "Product Level 02", 
                "Delivering Mill",
              "Document Item Currency",
              "Amount",
              "Currency")
    email_status=''
    
    status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
    if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)
        
        
        
    if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
    else: return {"status":"failure"},500


    

   
@smb_app2.route('/upload_extra_profile', methods=['GET','POST'])
@token_required
def upload_extra_profile():
    
            f=request.files['filename']
      
                
            f.save(input_directory+f.filename)
        
        
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","BusinessCode", "Market - Country",
       "Product Division", "Product Level 04", "Product Level 05",
       "Product Level 02", "Delivering Mill", "Document Item Currency",
       "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Market - Country",
       "Product Division", "Product Level 04", "Product Level 05",
       "Product Level 02", "Delivering Mill", "Document Item Currency",
       "Amount", "Currency" from "SMB"."SMB - Extra - Profile" where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
            
            
            df['Currency'] = df['Currency'].str.replace("'","")
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
       
@smb_app2.route('/validate_extra_profile', methods=['GET','POST'])
@token_required
def  validate_extra_profile():
    
        
    json_data=json.loads(request.data)
    username=request.headers['username']
        
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    df.insert(1,'date_time',date_time)
    # try:
       
    #     df=df[ ["Username","BusinessCode", "Market_Country",
    #    "Product_Division", "Product_Level_04", "Product_Level_05",
    #    "Product_Level_02", "Delivering_Mill", "Document_Item_Currency",
    #    "Amount", "Currency","date_time","sequence_id","id"]]
        
    #     query1='''INSERT INTO "SMB"."SMB - Extra - Profile_History"
    #     SELECT 
    #     "id",
    #     "Username",now(),
    #     "BusinessCode", "Market - Country",
    #    "Product Division", "Product Level 04", "Product Level 05",
    #    "Product Level 02", "Delivering Mill", "Document Item Currency",
    #    "Amount", "Currency"  FROM "SMB"."SMB - Extra - Profile"
    #     WHERE "id" in {} '''
        
    #     id_tuple=tuple(df["id"])
    #     if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
                    
    #     result=db.insert(query1.format(id_tuple))
    #     if result=='failed' :raise ValueError
        
        
    #     # looping for update query
    #     for i in range(0,len(df)):
    #         print(tuple(df.loc[0]))
    #         query2='''UPDATE "SMB"."SMB - Extra - Profile"
    #     SET 
    #    "Username"='%s',
    #    "BusinessCode"='%s', "Market - Country"='%s',
    #    "Product Division"='%s', "Product Level 04"='%s', "Product Level 05"='%s',
    #    "Product Level 02"='%s',
    #     "Delivering Mill"='%s',
    #    "Document Item Currency"='%s',
    #    "Amount"='%s',
    #    "Currency"=''%s'',
    #    "updated_on"='%s',
    #    sequence_id='%s'
    #     WHERE "id"= '%s' ''' % tuple(df.loc[i])
    #         result=db.insert(query2)
    #         if result=='failed' :raise ValueError
            
    #     return {"status":"success"},200
    # except:
    #     return {"status":"failure"},500
    tablename='SMB - Extra - Profile'
    df=fun_ignore_sequence_id(df,tablename)
    flag='update'  
    df.insert(1,'table_name',tablename)
    
    # df.insert(1,'table_name',tablename)
    col_tuple=("table_name",
               "id","sequence_id",
        "Username",
        "BusinessCode", 
        "Market - Country",
        "Product Division", 
        "Product Level 04", 
        "Product Level 05",
        "Product Level 02", 
        "Delivering Mill", 
        "Document Item Currency",
        "Amount", 
        "Currency")
    col_list=["table_name",
               "id","sequence_id",
        "Username",
        "BusinessCode", 
        "Market_Country",
        "Product_Division", 
        "Product_Level_04", 
        "Product_Level_05",
        "Product_Level_02", 
        "Delivering_Mill", 
        "Document_Item_Currency",
        "Amount", 
        "Currency"]
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
  
    return {"status":"success"},200
         

@smb_app2.route('/download_extra_profile',methods=['GET'])
def download_extra_profile():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select "id",sequence_id,"BusinessCode","Market - Country","Product Division","Product Level 04","Product Level 05","Product Level 02","Delivering Mill","Document Item Currency","Amount", "Currency" from "SMB"."SMB - Extra - Profile" where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'extra_profile.xlsx'
            
            df=df.style.apply(highlight_col, axis=None)
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
        except:
            return {"status":"failure"},500



# ***********************************************************************************************************************************************************************
# "SMB"."SMB - Extra - Profile - MiniBar"



@smb_app2.route('/data_extra_profile_minibar',methods=['GET','POST'])
@token_required
def  extra_profile_minibar():
    # query_paramters 
    search_string=request.args.get("search_string")
    
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
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Profile - MiniBar" where "active"='1' order by sequence_id  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=current_app.config['CON'])
        count=db.query('select count(*) from "SMB"."SMB - Extra - Profile - MiniBar" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
           
            
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app2.route('/delete_record_extra_profile_minibar',methods=['POST','GET','DELETE'])
@token_required
def delete_record_extra_profile_minibar():  
    
    id_value,username=json.loads(request.data)['id'],request.headers['username']
    
    
    tablename='SMB - Extra - Profile - MiniBar'
   
    status=email(id_value,tablename,'delete',username=username)
    if(status=='success'):return {"status":"success"},200
    else: return {"status":"failure"},500


@smb_app2.route('/get_record_extra_profile_minibar',methods=['GET','POST'])       
@token_required
def get_record_extra_profile_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Profile - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=current_app.config['CON'])
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
         
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app2.route('/add_record_extra_profile_minibar',methods=['POST'])
def add_record_extra_profile_minibar():
    
    
    username=request.headers['username']
       
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters["BusinessCode"])
    Customer_Group=(query_parameters['Customer_Group'])
    Market_Customer=(query_parameters['Market_Customer'])
   
    Market_Country=(query_parameters['Market_Country'])
    
    Product_Level_04=(query_parameters['Product_Level_04'])
    Product_Level_05=(query_parameters['Product_Level_05'])
    Product_Level_02=(query_parameters['Product_Level_02'])
    Delivering_Mill=(query_parameters['Delivering_Mill'])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    # try:
    #     input_tuple=( username,BusinessCode,Customer_Group,Market_Country,Product_Level_04,Product_Level_05,Product_Level_02,Delivering_Mill,Document_Item_Currency, Amount, Currency.strip("'"))
        
    #     query='''INSERT INTO "SMB"."SMB - Extra - Profile - MiniBar"(
             
    #          "Username",
             
        #       "BusinessCode",
        #       "Customer Group",
     
        # "Market - Country", 
       
        # "Product Level 04", 
        # "Product Level 05",
        # "Product Level 02", 
        # "Delivering Mill",
        #       "Document Item Currency",
        #       "Amount",
        #       "Currency")
    #          VALUES{};'''.format(input_tuple)
         
       
    #     db.insert(query)
    #     return {"status":"success"},200
    # except:
    #     return {"status":"failure"},500
    flag='add'
   
    tablename='SMB - Extra - Profile - MiniBar'     
   
    input_tuple=(tablename,flag,username,BusinessCode,Customer_Group,Market_Customer,Market_Country,Product_Level_04,Product_Level_05,Product_Level_02,Delivering_Mill,Document_Item_Currency, Amount, Currency.strip("'"))
    col_tuple=("table_name",
               "flag",  
               "Username",             
             "BusinessCode",
             "Customer Group",
             "Market_Customer",
                "Market - Country", 
                "Product Level 04", 
                "Product Level 05",
                "Product Level 02", 
                "Delivering Mill",
                "Document Item Currency",
                "Amount",
                "Currency")
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
    


@smb_app2.route('/update_record_extra_profile_minibar',methods=['POST'])
@token_required
def update_record_extra_profile_minibar():
    
    
    username=request.headers['username']
       
    query_parameters =json.loads(request.data)
    
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    
    sequence_id=(query_parameters['sequence_id'])
    BusinessCode=(query_parameters["BusinessCode"])
    Customer_Group=(query_parameters['Customer_Group'])
    
    Market_Country=(query_parameters['Market_Country'])
    Market_Customer=(query_parameters['Market_Customer'])
    
    Product_Level_04=(query_parameters['Product_Level_04'])
    Product_Level_05=(query_parameters['Product_Level_05'])
    Product_Level_02=(query_parameters['Product_Level_02'])
    Delivering_Mill=(query_parameters['Delivering_Mill'])
    id_value=(query_parameters['id_value'])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    # try:
        
    #     query1='''INSERT INTO "SMB"."SMB - Extra - Profile - MiniBar_History"
    #     SELECT 
    #     "id","Username",now(),"BusinessCode", "Customer Group",
    #    "Market - Customer", "Market - Country", "Product Level 04",
    #    "Product Level 05", "Product Level 02", "Delivering Mill",
    #    "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Profile - MiniBar"
    #     WHERE "id"={} '''.format(id_value)
    #     result=db.insert(query1)
    #     print(query1)
    #     if result=='failed' :raise ValueError
    
    #     query2='''UPDATE "SMB"."SMB - Extra - Profile - MiniBar"
    #     SET 
    #    "Username"='{0}',
    #    "BusinessCode"='{1}',
    #    "Customer Group"='{2}',
       
    #    "Market - Country"='{3}',
    #   	   "Product Level 04"='{4}',
    #    "Product Level 05"='{5}',
    #    "Product Level 02"='{6}',
    #    "Delivering Mill"='{7}',
    #    "Document Item Currency"='{8}',
    #    "Amount"='{9}',
    #    "Currency"=''{10}'',
    #    "updated_on"='{11}',sequence_id={12}
    #     WHERE "id"={13} '''.format(username,BusinessCode,Customer_Group,Market_Country,Product_Level_04,Product_Level_05,Product_Level_02,Delivering_Mill,Document_Item_Currency,Amount,Currency,date_time,sequence_id,id_value)
    #     result1=db.insert(query2)
    #     print(query1)
    #     if result1=='failed' :raise ValueError
        
    #     print("*****************************")
        
    #     return {"status":"success"},200
    # except:
    #     return {"status":"failure"},500
    flag='update'
      
    tablename='SMB - Extra - Profile - MiniBar'  
    
    input_tuple=(tablename,id_value,sequence_id,username,BusinessCode,Customer_Group,Market_Customer,Market_Country,Product_Level_04,Product_Level_05,Product_Level_02,Delivering_Mill,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name","id","sequence_id", "Username",
            "BusinessCode",
            "Customer Group",
       "Market - Customer",
        "Market - Country", 
        "Product Level 04",
        "Product Level 05", 
        "Product Level 02", 
        "Delivering Mill",
        "Document Item Currency", 
        "Amount", 
        "Currency")      
        
    
    email_status=''
        
    status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
    if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)
    
    if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
    else: return {"status":"failure"},500


    
   
@smb_app2.route('/upload_extra_profile_minibar', methods=['GET','POST'])
@token_required
def upload_extra_profile_minibar():
    
            f=request.files['filename']
      
                
            f.save(input_directory+f.filename)
            
        
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","BusinessCode", "Customer Group","Market - Customer",
       "Market - Country", "Product Level 04",
       "Product Level 05", "Product Level 02", "Delivering Mill",
       "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Customer Group","Market - Customer",
     "Market - Country", "Product Level 04",
       "Product Level 05", "Product Level 02", "Delivering Mill",
       "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Profile - MiniBar" where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
            
            
            df['Currency'] = df['Currency'].str.replace("'","")
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            print(df3)
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        
@smb_app2.route('/validate_extra_profile_minibar', methods=['GET','POST'])
@token_required
def  validate_extra_profile_minibar():
    
        
    json_data=json.loads(request.data)
    username=request.headers['username']
        
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    df.insert(1,'date_time',date_time)
    # try:
       
    #     df=df[ ["Username","BusinessCode", "Customer_Group",
    #    "Market_Country", "Product_Level_04",
    #    "Product_Level_05", "Product_Level_02", "Delivering_Mill",
    #    "Document_Item_Currency", "Amount", "Currency","date_time","sequence_id","id"]]
        
    #     query1='''INSERT INTO "SMB"."SMB - Extra - Profile - MiniBar_History" 
    #     SELECT 
    #     "id",
    #     "Username",now(),
    #     "BusinessCode", "Customer Group",
    #    "Market - Customer", "Market - Country", "Product Level 04",
    #    "Product Level 05", "Product Level 02", "Delivering Mill",
    #    "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Profile - MiniBar"
    #     WHERE "id" in {} '''
    #     id_tuple=tuple(df["id"])
    #     if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
                    
    #     query1=query1.format(id_tuple)
        
    #     print(query1)
    #     result=db.insert(query1)
    #     if result=='failed' :raise ValueError
        
        
    #     # looping for update query
    #     for i in range(0,len(df)):
    #         print(tuple(df.loc[0]))
    #         query2='''UPDATE "SMB"."SMB - Extra - Profile - MiniBar"
    #     SET 
    #    "Username"='%s',
    #    "BusinessCode"='%s',"Customer Group"='%s', "Market - Country"='%s',
    #    "Product Level 04"='%s',"Product Level 05"='%s' ,"Product Level 02"='%s',"Delivering Mill"='%s', 
       
    #    "Document Item Currency"='%s',
    #    "Amount"='%s',
    #    "Currency"=''%s'',
    #    "updated_on"='%s',sequence_id='%s'
    #     WHERE "id"= '%s' ''' % tuple(df.loc[i])
    #         print(query2)
    #         result=db.insert(query2)
    #         if result=='failed' :raise ValueError
            
    #     return {"status":"success"},200
    # except:
    #     return {"status":"failure"},500
    tablename="SMB - Extra - Profile - MiniBar"
    df=fun_ignore_sequence_id(df,tablename)
    flag='update'  
        
    df.insert(1,'table_name',tablename)

    col_tuple=("table_name",
               "id",
               "sequence_id",
        "Username",
        "BusinessCode", 
        "Customer Group",
       
        "Market - Customer",
        "Market - Country", 
        "Product Level 04",
        "Product Level 05", 
        "Product Level 02", 
        "Delivering Mill",
        "Document Item Currency", 
        "Amount", 
        "Currency")
    col_list=["table_name",
               "id",
               "sequence_id",
        "Username",
        "BusinessCode", 
        "Customer_Group",
        "Market_Customer",
        "Market_Country", 
        "Product_Level_04",
        "Product_Level_05", 
        "Product_Level_02", 
        "Delivering_Mill",
        "Document_Item_Currency", 
        "Amount", 
        "Currency"]
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
  
    return {"status":"success"},200
         

@smb_app2.route('/download_extra_profile_minibar',methods=['GET'])
def download_extra_profile_minibar():
    
        now = datetime.now()
        try:
            df = pd.read_sql('''select id,sequence_id,"BusinessCode","Customer Group","Market - Customer","Market - Country","Product Level 04","Product Level 05","Product Level 02","Delivering Mill","Document Item Currency","Amount", "Currency" from "SMB"."SMB - Extra - Profile - MiniBar" where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'extra_profile_minibar.xlsx'
            
            df=df.style.apply(highlight_col, axis=None)
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
        except:
            return {"status":"failure"},500
   
# ***********************************************************************************************************************************************************************
#  "SMB"."SMB - Extra - Profile Iberia and Italy"



@smb_app2.route('/data_extra_profile_Iberia',methods=['GET','POST'])
@token_required
def  extra_profile_minibar_iberia():
    # query_paramters 
    search_string=request.args.get("search_string")
    
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
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Profile Iberia and Italy" where "active"='1' order by sequence_id  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=current_app.config['CON'])
        count=db.query('select count(*) from "SMB"."SMB - Extra - Profile Iberia and Italy" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
           
            
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        
@smb_app2.route('/delete_record_extra_profile_Iberia',methods=['POST','GET','DELETE'])
@token_required
def delete_record_extra_profile_Iberia():  
   
    
    id_value,username=json.loads(request.data)['id'],request.headers['username']
    
    
    tablename='SMB - Extra - Profile Iberia and Italy'
   
    status=email(id_value,tablename,'delete',username=username)
    if(status=='success'):return {"status":"success"},200
    else: return {"status":"failure"},500


@smb_app2.route('/get_record_extra_profile_Iberia',methods=['GET','POST'])       
def get_record_extra_profile_Iberia():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Profile Iberia and Italy" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=current_app.config['CON'])
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
         
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app2.route('/add_record_extra_profile_Iberia',methods=['POST'])
@token_required
def add_record_extra_profile_Iberia():
    
    today = date.today()
    username=request.headers['username']
       
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters["BusinessCode"])
   
    Market_Country=(query_parameters['Market_Country'])
    Delivering_Mill=(query_parameters['Delivering_Mill'])
    Product_Level_02=(query_parameters['Product_Level_02'])
    Product_Level_05=(query_parameters['Product_Level_05'])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    # try:
    #     input_tuple=(username,BusinessCode,Market_Country,Delivering_Mill,Product_Level_02,Product_Level_05,Document_Item_Currency, Amount, Currency.strip("'"))
        
    #     query='''INSERT INTO "SMB"."SMB - Extra - Profile Iberia and Italy"(
            
    #          "Username",
             
        #       "BusinessCode",
             
        # "Market - Country", 
        # "Delivering Mill",
        # "Product Level 02", 
       
        # "Product Level 05",
       
      
        #       "Document Item Currency",
        #       "Amount",
        #       "Currency")
    #          VALUES{};'''.format(input_tuple)
         
       
    #     result=db.insert(query)
    #     if result=='failed':raise ValueError
    #     return {"status":"success"},200
    # except:
    #     return {"status":"failure"},500
    flag='add'
   
    tablename='SMB - Extra - Profile Iberia and Italy'     
   
    input_tuple=(tablename,flag,username,BusinessCode,Market_Country,Delivering_Mill,Product_Level_02,Product_Level_05,Document_Item_Currency, Amount, Currency.strip("'"))
    col_tuple=("table_name",
               "flag",  
               "Username",             
             "BusinessCode",             
            "Market - Country", 
            "Delivering Mill",
            "Product Level 02",        
            "Product Level 05",
              "Document Item Currency",
              "Amount",
              "Currency")
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500


@smb_app2.route('/update_record_extra_profile_Iberia',methods=['POST'])
@token_required
def update_record_extra_profile_Iberia():
    
    today = date.today()
    username=request.headers['username']
       
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters["BusinessCode"])
   
    Market_Country=(query_parameters['Market_Country'])
    Delivering_Mill=(query_parameters['Delivering_Mill'])
    Product_Level_02=(query_parameters['Product_Level_02'])
    Product_Level_05=(query_parameters['Product_Level_05'])
    id_value=(query_parameters['id_value'])
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    sequence_id=(query_parameters["sequence_id"])
    
    # try:
        
    #     query1='''INSERT INTO "SMB"."SMB - Extra - Profile Iberia and Italy_History"
    #     SELECT 
    #     "id",
    #     "Username",now(),"BusinessCode", "Market - Country",
    #    "Delivering Mill", "Product Level 02", "Product Level 05",
    #    "Document Item Currency" ,"Amount","Currency" FROM "SMB"."SMB - Extra - Profile Iberia and Italy"
    #     WHERE "id"={} '''.format(id_value)
    #     result=db.insert(query1)
    #     if result=='failed' :raise ValueError
    
    #     query2='''UPDATE "SMB"."SMB - Extra - Profile Iberia and Italy"
    #     SET 
    #    "Username"='{0}',
    #    "BusinessCode"='{1}',
    #    "Market - Country"='{2}',
    #   	   "Delivering Mill"='{3}',
    #    "Product Level 02"='{4}',
    #    "Product Level 05"='{5}',
    #    "Document Item Currency"='{6}',
    #    "Amount"='{7}',
    #    "Currency"=''{8}'',
    #    "updated_on"='{9}',
    #    sequence_id={10}
    #     WHERE "id"={11} '''.format(username,BusinessCode,Market_Country,Delivering_Mill,Product_Level_02,Product_Level_05,Document_Item_Currency,Amount,Currency,date_time,sequence_id,id_value)
    #     result1=db.insert(query2)
    #     if result1=='failed' :raise ValueError
        
    #     return {"status":"success"},200
    # except:
    #     return {"status":"failure"},500
    flag='update'
      
    tablename='SMB - Extra - Profile Iberia and Italy'  
    
    input_tuple=(tablename,id_value,sequence_id,username,BusinessCode,Market_Country,Delivering_Mill,Product_Level_02,Product_Level_05,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name",
               "id",
               "sequence_id",
            "Username",
        "BusinessCode", 
        "Market - Country",
        "Delivering Mill", 
        "Product Level 02", 
        "Product Level 05",
        "Document Item Currency" ,
        "Amount",
        "Currency")      
        
    
    email_status=''
        
    status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
    if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)

    
    if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
    else: return {"status":"failure"},500


    
    
    
   
@smb_app2.route('/upload_extra_profile_Iberia', methods=['GET','POST'])
@token_required
def upload_extra_profile_Iberia():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","BusinessCode", "Market - Country",
       "Delivering Mill", "Product Level 02", "Product Level 05",
       "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Market - Country",
       "Delivering Mill", "Product Level 02", "Product Level 05",
       "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Profile Iberia and Italy" where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
            
            df['Currency'] = df['Currency'].str.replace("'","")
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            print(df3)
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500


@smb_app2.route('/validate_extra_profile_Iberia', methods=['GET','POST'])
@token_required
def  validate_extra_profile_Iberia():
    
        
    json_data=json.loads(request.data)
    username=request.headers['username']
        
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    df.insert(1,'date_time',date_time)
    # try:
       
    #     df=df[ ["Username","BusinessCode", "Market_Country",
    #    "Delivering_Mill", "Product_Level_02", "Product_Level_05",
    #    "Document_Item_Currency", "Amount", "Currency","date_time","sequence_id","id"]]
        
    #     query1='''INSERT INTO "SMB"."SMB - Extra - Profile Iberia and Italy_History"
    #     SELECT 
    #     "id",
    #     "Username",now(),
    #     "BusinessCode", "Market - Country",
    #    "Delivering Mill", "Product Level 02", "Product Level 05",
    #    "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Profile Iberia and Italy"
    #     WHERE "id" in {} '''
        
    #     id_tuple=tuple(df["id"])
    #     if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
    #     result=db.insert(query1.format(id_tuple))
    #     if result=='failed' :raise ValueError
        
        
    #     # looping for update query
    #     for i in range(0,len(df)):
    #         print(tuple(df.loc[0]))
    #         query2='''UPDATE "SMB"."SMB - Extra - Profile Iberia and Italy"
    #     SET 
    #    "Username"='%s',
    #    "BusinessCode"='%s', "Market - Country"='%s',
    #    "Delivering Mill"='%s', "Product Level 02"='%s', "Product Level 05"='%s',
       
    #    "Document Item Currency"='%s',
    #    "Amount"='%s',
    #    "Currency"=''%s'',
    #    "updated_on"='%s',
    #    sequence_id='%s'
    #     WHERE "id"= '%s' ''' % tuple(df.loc[i])
    #         result=db.insert(query2)
    #         if result=='failed' :raise ValueError
            
    #     return {"status":"success"},200
    # except:
    #     return {"status":"failure"},500
    tablename='SMB - Extra - Profile Iberia and Italy'
    df=fun_ignore_sequence_id(df,tablename)
    flag='update'  
    
    df.insert(1,'table_name',tablename)

    col_tuple=("table_name",
               "id",
               "sequence_id",
          "Username",
        "BusinessCode", 
        "Market - Country",
        "Delivering Mill", 
        "Product Level 02", 
        "Product Level 05",
        "Document Item Currency", 
        "Amount", 
        "Currency")
    col_list=["table_name",
               "id",
               "sequence_id",
          "Username",
        "BusinessCode", 
        "Market_Country",
        "Delivering_Mill", 
        "Product_Level_02", 
        "Product_Level_05",
        "Document_Item_Currency", 
        "Amount", 
        "Currency"]
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
  
    return {"status":"success"},200


@smb_app2.route('/download_extra_profile_Iberia',methods=['GET'])
def download_extra_profile_Iberia():
        now = datetime.now()
        try:
            df = pd.read_sql('''select "id",sequence_id,"BusinessCode","Market - Country","Delivering Mill","Product Level 02","Product Level 05","Document Item Currency","Amount", "Currency" from "SMB"."SMB - Extra - Profile Iberia and Italy" where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'extra_profile_Iberia.xlsx'
            df=df.style.apply(highlight_col, axis=None)
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
        except:
            return {"status":"failure"},500
   

# ***********************************************************************************************************************************************************************
#  "SMB"."SMB - Extra - Profile Iberia and Italy minibar"



@smb_app2.route('/data_extra_profile_Iberia_minibar',methods=['GET','POST'])
@token_required
def  extra_profile_minibar_iberia_minibar():
    # query_paramters 
    search_string=request.args.get("search_string")
    
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
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Profile Iberia and Italy - MiniBar" where "active"='1' order by sequence_id  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=current_app.config['CON'])
        count=db.query('select count(*) from "SMB"."SMB - Extra - Profile Iberia and Italy - MiniBar" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
             
            
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app2.route('/delete_record_extra_profile_Iberia_minibar',methods=['POST','GET','DELETE'])

@token_required
def delete_record_extra_profile_Iberia_minibar():  
    
    
    id_value,username=json.loads(request.data)['id'],request.headers['username']
    
    
    tablename='SMB - Extra - Profile Iberia and Italy - MiniBar'
   
    status=email(id_value,tablename,'delete',username=username)
    if(status=='success'):return {"status":"success"},200
    else: return {"status":"failure"},500



@smb_app2.route('/get_record_extra_profile_Iberia_minibar',methods=['GET','POST'])       
@token_required
def get_record_extra_profile_Iberia_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Profile Iberia and Italy - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=current_app.config['CON'])
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
          
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app2.route('/add_record_extra_profile_Iberia_minibar',methods=['POST'])
@token_required
def add_record_extra_profile_Iberia_minibar():
    
    today = date.today()
    username=request.headers['username']
       
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    Market_Customer=query_parameters["Market_Customer"]
    BusinessCode=(query_parameters["BusinessCode"])
    Market_Country=(query_parameters['Market_Country'])
    Market_Customer_Group=(query_parameters['Market_Customer_Group'])
   
    Delivering_Mill=(query_parameters['Delivering_Mill'])
    Product_Level_02=(query_parameters['Product_Level_02'])
    Product_Level_05=(query_parameters['Product_Level_05'])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    # try:
        
        
    #     input_tuple=( username,BusinessCode,Market_Country,Market_Customer_Group,Delivering_Mill,Product_Level_02,Product_Level_05,Document_Item_Currency, Amount, Currency.strip("'"))
        
    #     query='''INSERT INTO "SMB"."SMB - Extra - Profile Iberia and Italy - MiniBar"(
             
    #          "Username",
            
        #       "BusinessCode",
             
        # "Market - Country",
        # "Market - Customer Group",

        # "Delivering Mill",
        # "Product Level 02", 
       
        # "Product Level 05",
       
      
        #       "Document Item Currency",
        #       "Amount",
        #       "Currency")
    #          VALUES{};'''.format(input_tuple)
         
       
    #     result=db.insert(query)
    #     if result=='failed':raise ValueError
    #     return {"status":"success"},200
    # except:
    #     return {"status":"failure"},500
    flag='add'
   
    tablename='SMB - Extra - Profile Iberia and Italy - MiniBar'     
   
    input_tuple=(tablename,flag,username,BusinessCode,Market_Country,Market_Customer_Group,Market_Customer,Delivering_Mill,Product_Level_02,Product_Level_05,Document_Item_Currency, Amount, Currency.strip("'"))
    col_tuple=("table_name",
               "flag",  
               "Username",             
             "BusinessCode",
            "Market - Country",
            "Market - Customer Group",
            "Market - Customer",
            "Delivering Mill",
            "Product Level 02",
            "Product Level 05",
              "Document Item Currency",
              "Amount",
              "Currency")
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
    


@smb_app2.route('/update_record_extra_profile_Iberia_minibar',methods=['POST'])
def update_record_extra_profile_Iberia_minibar():
    
    today = date.today()
    username=request.headers['username']
       
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    print(query_parameters)
    BusinessCode=(query_parameters["BusinessCode"])
    Market_Country=(query_parameters['Market_Country'])
    Market_Customer_Group=(query_parameters['Market_Customer_Group'])
   
    Delivering_Mill=(query_parameters['Delivering_Mill'])
    Product_Level_02=(query_parameters['Product_Level_02'])
    Market_Customer=query_parameters["Market_Customer"]
    Product_Level_05=(query_parameters['Product_Level_05'])
    id_value=(query_parameters['id_value'])
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    sequence_id=(query_parameters['sequence_id'])
    
    # try:
        
    #     query1='''INSERT INTO "SMB"."SMB - Extra - Profile Iberia and Italy - MiniBar_History" 
    #     SELECT 
    #     "id","Username",now(),"BusinessCode", "Market - Country",
    #    "Market - Customer Group", "Market - Customer", "Delivering Mill",
    #    "Product Level 02", "Product Level 05", "Document Item Currency",
    #    "Amount", "Currency"  FROM "SMB"."SMB - Extra - Profile Iberia and Italy - MiniBar"
    #     WHERE "id"={} '''.format(id_value)
    #     result=db.insert(query1)
    #     if result=='failed' :raise ValueError
    
    #     query2='''UPDATE "SMB"."SMB - Extra - Profile Iberia and Italy - MiniBar"
    #     SET 
    #    "Username"='{0}',
    #    "BusinessCode"='{1}',
    #    "Market - Country"='{2}',
    #    "Market - Customer Group"='{3}',
      
    #    "Delivering Mill"='{4}',
       
      	   
    #    "Product Level 02"='{5}',
    #    "Product Level 05"='{6}',
    #    "Document Item Currency"='{7}',
    #    "Amount"='{8}',
    #    "Currency"=''{9}'',
    #    "updated_on"='{10}',sequence_id={11}
    #     WHERE "id"={12} '''.format(username,BusinessCode,Market_Country,Market_Customer_Group,Delivering_Mill,Product_Level_02,Product_Level_05,Document_Item_Currency,Amount,Currency,date_time,sequence_id,id_value)
    #     result1=db.insert(query2)
    #     if result1=='failed' :raise ValueError
    #     print(query1)
    #     print("*****************************")
    #     print(query2)
    #     return {"status":"success"},200
    # except:
    #     return {"status":"failure"},500
    flag='update'
      
    tablename='SMB - Extra - Profile Iberia and Italy - MiniBar'  
    
    input_tuple=(tablename,id_value,sequence_id,username,BusinessCode,Market_Country,Market_Customer_Group,Market_Customer,Delivering_Mill,Product_Level_02,Product_Level_05,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name",
               "id",
               "sequence_id", 
               "Username",
                "BusinessCode", 
                "Market - Country",
                "Market - Customer Group", 
                "Market - Customer",
                "Delivering Mill",
                "Product Level 02", 
                "Product Level 05", 
                "Document Item Currency",
                "Amount", 
                "Currency")      
        
    
    email_status=''
        
    status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
    if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)
    
    
    if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
    else: return {"status":"failure"},500


     

   
@smb_app2.route('/upload_extra_profile_Iberia_minibar', methods=['GET','POST'])
@token_required
def upload_extra_profile_Iberia_minibar():
    
            f=request.files['filename']
      
                
            f.save(input_directory+f.filename)
        
        
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","BusinessCode", "Market - Country",
       "Market - Customer Group","Market - Customer", "Delivering Mill",
       "Product Level 02", "Product Level 05", "Document Item Currency",
       "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Market - Country",
       "Market - Customer Group", "Market - Customer", "Delivering Mill",
       "Product Level 02", "Product Level 05", "Document Item Currency",
       "Amount", "Currency" from "SMB"."SMB - Extra - Profile Iberia and Italy - MiniBar" where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
            
            
            df['Currency'] = df['Currency'].str.replace("'","")
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
             
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
       

@smb_app2.route('/validate_extra_profile_Iberia_minibar', methods=['GET','POST'])
@token_required
def  validate_extra_profile_Iberia_minibar():
    
        
    json_data=json.loads(request.data)
    username=request.headers['username']
        
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    df.insert(1,'date_time',date_time)
    # try:
       
    #     df=df[ ["Username","BusinessCode", "Market_Country",
    #    "Market_Customer_Group",  "Delivering_Mill",
    #    "Product_Level_02", "Product_Level_05", "Document_Item_Currency",
    #    "Amount", "Currency","date_time","sequence_id","id"]]
        
    #     query1='''INSERT INTO "SMB"."SMB - Extra - Profile Iberia and Italy - MiniBar_History"
    #     SELECT 
    #     "id",
    #     "Username",now(),
    #     "BusinessCode", "Market - Country",
    #    "Market - Customer Group", "Market - Customer", "Delivering Mill",
    #    "Product Level 02", "Product Level 05", "Document Item Currency",
    #    "Amount", "Currency"  FROM "SMB"."SMB - Extra - Profile Iberia and Italy - MiniBar"
    #     WHERE "id" in {} '''
        
    #     id_tuple=tuple(df["id"])
    #     if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
    #     result=db.insert(query1.format(id_tuple))
    #     print(query1.format(id_tuple))
    #     if result=='failed' :raise ValueError
        
    #     # looping for update query
    #     for i in range(0,len(df)):
    #         print(tuple(df.loc[0]))
    #         query2='''UPDATE "SMB"."SMB - Extra - Profile Iberia and Italy - MiniBar"
    #     SET 
    #    "Username"='%s',
    #    "BusinessCode"='%s', "Market - Country"='%s',
    #    "Market - Customer Group"='%s', "Delivering Mill"='%s',
    #    "Product Level 02"='%s', "Product Level 05"='%s',
    #    "Document Item Currency"='%s',
    #    "Amount"='%s',
    #    "Currency"=''%s'',
    #    "updated_on"='%s',"sequence_id"='%s'
    #     WHERE "id"= '%s' ''' % tuple(df.loc[i])
    #         result=db.insert(query2)
    #         if result=='failed' :raise ValueError
            
    #     return {"status":"success"},200
    # except:
    #     return {"status":"failure"},500
    tablename='SMB - Extra - Profile Iberia and Italy - MiniBar'
    df=fun_ignore_sequence_id(df,tablename)
    
    flag='update'
    df.insert(1,'table_name',tablename)
    col_tuple=("table_name",
               "id",
               "sequence_id",
          "Username",
        "BusinessCode", 
        "Market - Country",
        "Market - Customer Group", 
        "Market - Customer",
      
        "Delivering Mill",
        "Product Level 02", 
        "Product Level 05", 
        "Document Item Currency",
        "Amount", 
        "Currency")
    col_list=["table_name",
               "id",
               "sequence_id",
              "Username",
            "BusinessCode", 
            "Market_Country",
            "Market_Customer_Group", 
            "Market_Customer",
            
            "Delivering_Mill",
            "Product_Level_02", 
            "Product_Level_05", 
            "Document_Item_Currency",
            "Amount", 
            "Currency"]
    
    id_value=[]
  
    df=df[col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
  
    return {"status":"success"},200
    
@smb_app2.route('/download_extra_profile_Iberia_minibar',methods=['GET'])
def download_extra_profile_Iberia_minibar():
        now = datetime.now()
        try:
            df = pd.read_sql('''select id,sequence_id,"BusinessCode","Market - Country","Market - Customer Group","Market - Customer","Delivering Mill","Product Level 02","Product Level 05","Document Item Currency","Amount", "Currency" from "SMB"."SMB - Extra - Profile Iberia and Italy - MiniBar" where "active"='1' order by sequence_id ''', con=current_app.config['CON'])
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'extra_profile_Iberia_minibar.xlsx'
            
            df=df.style.apply(highlight_col, axis=None)
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
        except:
            return {"status":"failure"},500
   
        







