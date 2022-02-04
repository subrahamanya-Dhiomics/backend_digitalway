# -*- coding: utf-8 -*-
"""
Created on Thu Oct  12 07:33:38 2021

@author: subbu

"""

from flask import Blueprint
import pandas as pd
import time
import json
from flask import Flask, request, send_file, render_template, make_response,current_app
from flask import jsonify
from flask_cors import CORS
from json import JSONEncoder
from collections import OrderedDict
from flask import Blueprint
import psycopg2
import shutil
from functools import wraps
from pathlib import Path
import os
from sqlalchemy import create_engine
import getpass
import cryptocode
from datetime import datetime,date
from smb_phase2 import token_required
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText



import smb_phase2
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
               
 
smb_app1 = Blueprint('smb_app1', __name__)

CORS(smb_app1)

db=Database()


def tuple_to_string(col_tuple):
    columnstr=''' '''
    for i in col_tuple:
             if(i!=col_tuple[-1]):  columnstr+=''' "{}",'''.format(i)
             else: columnstr+=''' "{}" '''.format(i)   
    return columnstr
    






def upsert(col_tuple,input_tuple,flag='update',tablename=None,id_value=None):
        querystr=''' '''
        columnstr=tuple_to_string(col_tuple)
        tableid=''
       
        
        if(flag=='update'):
            check=db.query(''' select exists(select 1  from "SMB"."SMB_Aproval" where id={} and table_name='{}') '''.format(id_value,tablename))[0][0]
            if check and check!='failed':querystr=''' UPDATE "SMB"."SMB_Aproval" SET ({})= {} where id={} and table_name='{}' '''.format(columnstr,input_tuple,id_value,tablename)
            else: querystr='''INSERT INTO "SMB"."SMB_Aproval" ({}) VALUES{}; '''.format(columnstr,input_tuple)
            print(querystr)
            status=db.insert(querystr)
            
           
        
           
            tableid=db.query(''' select tableid from "SMB"."SMB_Aproval" where id={} and table_name='{}' '''.format(id_value,tablename))[0][0]
          
            
            
        if(flag=='add'):
            querystr=''' Insert into  "SMB"."SMB_Aproval"  ({}) VALUES{}'''.format(columnstr,input_tuple)
            print(querystr)
            status=db.insert(querystr)
            
            
            tableid=db.query(''' select max(tableid) from "SMB"."SMB_Aproval" where table_name='{}' and flag='add' '''.format(tablename) )[0][0]
            
               
        if status!='failed' : status='success'
        return {"status":status,"tableid":tableid}
    
def move_records(tablename,col_tuple,value_tuple,flag,id_value=None):
    querystr=''' '''
    columnstr=tuple_to_string(col_tuple)
   
    if(flag=='update'):
        
        query1=''' INSERT INTO "SMB"."{}_History"({})
        SELECT 
        
        {}
    
         FROM "SMB"."{}"
        WHERE "id" ={} '''.format(tablename,columnstr,columnstr,tablename,id_value)
        
        
        query2= ''' UPDATE "SMB"."{}" SET ({})= {} where id={}  '''.format(tablename,columnstr,value_tuple,id_value)
        
        db.insert(query1)
        status=db.insert(query2)
        if(status!='failed'):
            db.insert(''' delete from "SMB"."SMB_Aproval" where id={} '''.format(id_value))
            status='success'
        

        
        return status
    if(flag=='add'):
         
        query='''INSERT INTO "SMB"."{}"(
             
            {})
             VALUES{};'''.format(tablename,columnstr,value_tuple)
        status=db.insert(query)
        if(status!='failed'):
            db.insert(''' delete from "SMB"."SMB_Aproval" where tableid={} '''.format(id_value))
            status='success'
        
        return status
    

def email(id_value,tablename):  
      
        encoded_id = cryptocode.encrypt(str(id_value),current_app.config["mypassword"])
        ## And then to decode it:
        
        approver='subrahamanya.shetty@dhiomics.com'
        mail_from='''subrahamanya@digitalway-lu.com'''
       
        msg = MIMEMultipart('alternative')
        msg['Subject'] = "Approve Records"
        msg['From'] =mail_from
        msg['To'] = approver
        #print(user)
        html=''
        with open('email_aproval.html', 'r') as f:
         html = f.read()
        print("*********")
        
        print(html)
        
        
        print("**")
        html=html.format(tablename,'admin','TEST3221',encoded_id,tablename)
       
        
        
        part = MIMEText(html, 'html')
        msg.attach(part)
        server = smtplib.SMTP('smtp.gmail.com',587)
        server.ehlo()
        server.starttls()
        server.login(mail_from, '78df@$M80')
        server.sendmail(mail_from, approver, msg.as_string())     
        server.close()
        return 'success'
   
        
        
download_path="/home/ubuntu/mega_dir/"
input_directory="/home/ubuntu/mega_dir/"


# download_path="C:/Users/Administrator/Documents/"
# input_directory="C:/Users/Administrator/Documents/"


con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')



@smb_app1.route('/hello_world',methods=['GET'])

def Hello():
    
    return {"status":"success"}



@smb_app1.route('/Base_Price_Data',methods=['GET','POST'])
@token_required
def SMB_data():
    
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
        query='''select * from "SMB"."SMB - Base Price - Category Addition" where "active"='1' order by sequence_id  OFFSET {} LIMIT {} '''.format(lowerLimit,upperLimit) 
        df = pd.read_sql(query, con=con)
        
        count=db.query('select count(*) from "SMB"."SMB - Base Price - Category Addition" where "active"=1 ')[0][0]
        
        
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

@smb_app1.route('/delete_record_baseprice',methods=['POST','GET','DELETE'])
@token_required

def delete_record():  
    id_value=json.loads(request.data)['id']
    id_value.append(0)
    id_value=tuple(id_value)
    try:
        query='''UPDATE "SMB"."SMB - Base Price - Category Addition" SET "active"=0 WHERE "id" in {}'''.format(id_value)
        result=db.insert(query)
        print(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
   
@smb_app1.route('/get_record_baseprice',methods=['GET','POST'])  
@token_required     
def get_record():
    id_value=request.args.get('id')  
    
    query='''select * from "SMB"."SMB - Base Price - Category Addition" where "id"={} '''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500



@smb_app1.route('/update_record_baseprice',methods=['POST'])
@token_required
def update_record1():
        username = getpass.getuser()
        now = datetime.now()
        date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
        
        query_parameters =json.loads(request.data) 
        
        sequence_id=(query_parameters['sequence_id'])
        BusinessCode=(query_parameters['BusinessCode'])
        Market_Country =( query_parameters["Market_Country"])
        Product_Division =( query_parameters["Product_Division"])
        Product_Level_02 =( query_parameters["Product_Level_02"])
        Document_Item_Currency =( query_parameters["Document_Item_Currency"])
        Amount =( query_parameters["Amount"])
        Currency =( query_parameters["Currency"])
        id_value=(query_parameters['id_value'])
        sequence_id=(query_parameters['sequence_id'])
        
        flag='update'
      
        tablename='SMB - Base Price - Category Addition'        
        
        input_tuple=(tablename,id_value,sequence_id,username, BusinessCode,  Market_Country, Product_Division,Product_Level_02,Document_Item_Currency, Amount,Currency)
        
        col_tuple=("table_name","id","sequence_id",
             "Username",
             "BusinessCode",
             "Market - Country",
             "Product Division",
             "Product Level 02",
             "Document Item Currency",
             "Amount",
             "Currency")
        email_status=''
        
        status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
        if(status['status']=='success'):email_status=email([status['tableid']],tablename)
        
        
        
        if(email_status=='success'): return {"status":"success"},200
        else: return {"status":"failure"},500



          
          
#         query=''' INSERT INTO "SMB"."SMB - Base Price - Category Addition"(
#              id,
#              "Username",
#              "BusinessCode",
#              "Market - Country",
#              "Product Division",
#              "Product Level 02",
#              "Document Item Currency",
#              "Amount",
#              "Currency")
#              VALUES {}
# 			 ON CONFLICT (id) 
# 			 DO 
#    update set "Username"=excluded."Username" ,"BusinessCode"=exluded."BusinessCode","Market - Country"=excluded."Market - Country","Product Division"=excluded."Product Division", ; '''.format(input_tuple)
        
    
        
        
        
        
#         query1='''INSERT INTO "SMB"."SMB - Base Price - Category Addition_History" 
#         SELECT 
#         "id","Username","BusinessCode","Market - Country",
#       	   "Product Division",
#        "Product Level 02",
#        "Document Item Currency",
#        "Amount",
#        "Currency"  FROM "SMB"."SMB - Base Price - Category Addition"
#         WHERE "id"={} '''.format(id_value)
#         result=db.insert(query1)
        
#         if result=='failed' :raise ValueError
    
#         query2='''UPDATE "SMB"."SMB - Base Price - Category Addition"
#         SET 
#        "Username"='{0}',
#        "BusinessCode"='{1}',
#        "Market - Country"='{2}',
#       	   "Product Division"='{3}',
#        "Product Level 02"='{4}',
#        "Document Item Currency"='{5}',
#        "Amount"='{6}',
#        "Currency"=''{7}'',
#        "updated_on"='{8}'
       
#         WHERE "id"={9} '''.format(username,BusinessCode,Market_Country,Product_Division,Product_Level_02,Document_Item_Currency,Amount,Currency,date_time,id_value)
#         result1=db.insert(query2)
#         if result1=='failed' :raise ValueError
#         return {"status":"success"},200
   

@smb_app1.route('/add_record_baseprice',methods=['POST'])
@token_required
def add_record1():
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    
    query_parameters =json.loads(request.data)
    BusinessCode=(query_parameters['BusinessCode'])
    Market_Country =( query_parameters["Market_Country"])
    Product_Division =( query_parameters["Product_Division"])
    Product_Level_02 =( query_parameters["Product_Level_02"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    flag='add'
   
    tablename='SMB - Base Price - Category Addition'
    # id_value=db.query('''select "SMB".get_max_id('"SMB"."SMB - Base Price - Category Addition"')''')[0][0]+1
    
    
    
    input_tuple=(tablename,flag,username, BusinessCode,  Market_Country, Product_Division,Product_Level_02,Document_Item_Currency, Amount,Currency)
    col_tuple=("table_name","flag", "Username",
              "BusinessCode",
              "Market - Country",
              "Product Division",
              "Product Level 02",
              "Document Item Currency",
              "Amount",
              "Currency")
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename)
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
        

      
        
    # try:
    #     input_tuple=(flag,username, BusinessCode,  Market_Country, Product_Division,Product_Level_02,Document_Item_Currency, Amount,Currency.strip("'"))
    #     query='''INSERT INTO "SMB"."SMB_Aproval"(
             
              
    #          "Username",
    #          "BusinessCode",
    #          "Market - Country",
    #          "Product Division",
    #          "Product Level 02",
    #          "Document Item Currency",
    #          "Amount",
    #          "Currency")
    #          VALUES{};'''.format(input_tuple)
    #     print(query)
    #     result=db.insert(query)  
    #     if result=='failed' :raise ValueError
        
    #     return {"status":"success"},200
        
    # except:
    #     return {"status":"failure"},500

@smb_app1.route('/aproval_data',methods=['GET','POST'])
def aproval_data():
    id_value=request.args.get('id_value') 
    id_value=id_value.replace(' ','+')
    id_value = cryptocode.decrypt(id_value, current_app.config["mypassword"])
    id_list=eval(id_value)
    id_list.append(0)
    id_tuple=tuple(id_list)
    data=[{}]
    
    tablename=request.args.get('tablename')
    
    query='''select * from "SMB"."SMB_Aproval" where tableid in {} and table_name='{}' '''.format(id_tuple,tablename)
    df=pd.read_sql(query,con=con)
    try:
        if(len(df)<1):raise ValueError
          
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Zip_Code_(Dest)":"Zip_Code_Dest"},inplace=True)
        df.dropna(axis=1, how='all',inplace=True)
        data=json.loads(df.to_json(orient='records'))
        
        return {"data":data},200
    except:
        data=[]
        return {"data": data},200
          
 

@smb_app1.route('/aprove_records',methods=['GET','POST'])
def aprove_records():
    data=json.loads(request.data)
    df=pd.DataFrame(data["data"])
    tablename=data['tablename']
    print(data)
  
    for i in range(0,len(df)):
        flag=df['flag'][i]
         
        tableid=df['tableid'][i]
        try:
            id_value=df['id'][i]
            
        except:pass
       
        if(flag=='update'):
         df2=df.drop(['tableid','flag','updated_on'], axis = 1)
         col_tuple=tuple(list(df2.columns))
         value_tuple=tuple(df2.loc[i])
         status=move_records(tablename,col_tuple,value_tuple,flag,id_value)
       
        else:
           df2=df.drop(['tableid','flag','updated_on'], axis = 1)
           col_tuple=tuple(list(df2.columns))
           value_tuple=tuple(df2.loc[i])
           status=move_records(tablename,col_tuple,value_tuple,flag,tableid)
        
    return {"status":"success"},200

@smb_app1.route('/reject_records',methods=['GET','POST'])
def reject_records():
    data=json.loads(request.data)
    tablename=data['tablename']
    id_value=data['id_value']
    id_value=id_value.replace(' ','+')
    id_value = cryptocode.decrypt(id_value, current_app.config["mypassword"])
    id_list=eval(id_value)
    id_list.append(0)
    id_tuple=tuple(id_list) 
    query=''' delete from "SMB"."SMB_Aproval" where tableid in {} '''.format(id_tuple)
    db.insert(query)
    return {"status":"success"},200
    
    
    



   
@smb_app1.route('/Base_Price_Upload', methods=['GET','POST'])

def  SMB_upload():
    
            f=request.files['filename']
      
                
            f.save(input_directory+f.filename)
            
            
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[['id','BusinessCode','Market - Country','Product Division','Product Level 02','Document Item Currency', 'Amount', 'Currency','sequence_id']]  
            df['id']=df['id'].astype(int)
            df['sequence_id']=df['sequence_id'].astype(int)
            
            df_main = pd.read_sql('''select "id","BusinessCode","Market - Country","Product Division","Product Level 02","Document Item Currency", "Amount", "Currency" ,sequence_id from "SMB"."SMB - Base Price - Category Addition" where "active"='1' order by "id" ''', con=con)
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200



@smb_app1.route('/Base_Price_validate', methods=['GET','POST'])
@token_required
def  SMB_validate():
    json_data=json.loads(request.data)
    username = getpass.getuser() 
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    email_status=''
    df.insert(0,'Username',username)
    
    tablename='SMB - Base Price - Category Addition' 
    flag='update'  
    df.insert(1,'table_name',tablename)
    col_tuple=("table_name","id","sequence_id",
             "Username",
             "BusinessCode",
             "Market - Country",
             "Product Division",
             "Product Level 02",
             "Document Item Currency",
             "Amount",
             "Currency")
    col_list=['table_name','id','sequence_id','Username','BusinessCode','Market_Country','Product_Division','Product_Level_02','Document_Item_Currency', 'Amount', 'Currency']
    
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename)
  
    return {"status":"success"},200
   
     

         
@smb_app1.route('/Base_price_download',methods=['GET'])
def SMB_baseprice_download1():
   
        now = datetime.now()
       
        df = pd.read_sql('''select * from "SMB"."SMB - Base Price - Category Addition" where "active"='1' order by sequence_id ''', con=con)
        df.drop(['Username','updated_on','active','aprover1','aprover2','aprover3'],axis=1,inplace=True)
        t=now.strftime("%d-%m-%Y-%H-%M-%S")
        file=download_path+t+'baseprice_category_addition.xlsx'
        
        
        df.to_excel(file,index=False)
        
        return send_file(file, as_attachment=True)
       
        
   
  
       
        
    
# ***************************************************************************************************************************************************************************************
# baseprice_minibar

@smb_app1.route('/data_baseprice_category_minibar',methods=['GET','POST'])

@token_required
def SMB_data_baseprice_mini():
    # query_paramters 
    search_string=request.args.get("search_string")  
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    

    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
   
    # fetching the data from database and filtering    
    try:
        query='''select * from "SMB"."SMB - Base Price - Category Addition - MiniBar" where "active"='1' order by sequence_id  OFFSET {} LIMIT {} '''.format(lowerLimit,upperLimit) 
        df = pd.read_sql(query, con=con)
        
        
        count=db.query('select count(*) from "SMB"."SMB - Base Price - Category Addition - MiniBar" where "active"=1 ')[0][0]
        
        
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
  
@smb_app1.route('/delete_record_baseprice_category_minibar',methods=['POST','GET','DELETE'])
@token_required
def delete_record_baseprice_mini():  
    id_value=json.loads(request.data)['id']
    id_value.append(0)
    id_value=tuple(id_value)
    try:
        query='''UPDATE "SMB"."SMB - Base Price - Category Addition - MiniBar" SET "active"=0 WHERE "id" in {} '''.format(id_value)
        result=db.insert(query)
        
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app1.route('/get_record_baseprice_category_minibar',methods=['GET','POST'])     
@token_required  
def get_record_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Base Price - Category Addition - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    

@smb_app1.route('/update_record_baseprice_category_minibar',methods=['POST'])
@token_required
def update_record_category_minibar():
        username = getpass.getuser()
        now = datetime.now()
        date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
        query_parameters =json.loads(request.data) 
        
        BusinessCode=(query_parameters['BusinessCode'])
        Customer_Group=(query_parameters["Customer_Group"])
       
        Market_Country =( query_parameters["Market_Country"])
        Beam_Category=(query_parameters["Beam_Category"])
        
        
        
        Document_Item_Currency =( query_parameters["Document_Item_Currency"])
        Amount =( query_parameters["Amount"])
        Currency =( query_parameters["Currency"])
        sequence_id=(query_parameters['sequence_id'])
        id_value=(query_parameters['id_value'])
    
    
    
    
        
        flag='update'
      
        tablename='SMB - Base Price - Category Addition - MiniBar'    
        
        input_tuple=(tablename,id_value,sequence_id,username,BusinessCode,Customer_Group,Market_Country,Beam_Category,Document_Item_Currency,Amount,Currency)
        col_tuple=("table_name","id","sequence_id",
             "Username",
             "BusinessCode",
             "Customer Group",
            
             "Market - Country",
        	 "Beam Category",
           "Document Item Currency",
           "Amount",
           "Currency")
        email_status=''
        
        status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
        if(status['status']=='success'):email_status=email([status['tableid']],tablename)
        
        
        
        if(email_status=='success'): return {"status":"success"},200
        else: return {"status":"failure"},500
    

   


@smb_app1.route('/add_record_baseprice_category_minibar',methods=['POST'])
@token_required
def add_record_mini():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Customer_Group=(query_parameters["Customer_Group"])
   
    Market_Country =( query_parameters["Market_Country"])
    Beam_Category=(query_parameters["Beam_Category"])
    
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
     
    # id_value=db.query('''select "SMB".get_max_id('"SMB"."SMB - Base Price - Category Addition"')''')[0][0]+1
    
    flag='add'
   
    tablename='SMB - Base Price - Category Addition - MiniBar'    
       
    
    input_tuple=(tablename,flag,username,BusinessCode,Customer_Group,Market_Country,Beam_Category,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name","flag","Username",
             "BusinessCode",
             "Customer Group",
         
             "Market - Country",
        	 "Beam Category",
           "Document Item Currency",
           "Amount",
           "Currency")
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename)
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
        

 
   
@smb_app1.route('/upload_baseprice_category_minibar', methods=['GET','POST'])

def  SMB_upload_baseprice_minibar():
            print("step1 : here")
            request.files['filename']
            
            f=request.files['filename']
            print("step2 ",f.filename)
            
             
            
            
            f.save(input_directory+f.filename)
            
            print("step3",type(f))
        
        
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[['id','BusinessCode', 'Customer Group', 'Market - Country', 'Beam Category','Document Item Currency', 'Amount', 'Currency','sequence_id']]  
            df['id']=df['id'].astype(int)
            df["sequence_id"]=df["sequence_id"].astype(int)
           
            df_main = pd.read_sql('''select "id","BusinessCode", "Customer Group", "Market - Country", "Beam Category","Document Item Currency", "Amount", "Currency",sequence_id from "SMB"."SMB - Base Price - Category Addition - MiniBar" where "active"='1' order by sequence_id ''', con=con)
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
       


@smb_app1.route('/validate_baseprice_category_minibar', methods=['GET','POST'])
@token_required
def  SMB_validate_baseprice_minibar():
    json_data=json.loads(request.data)
    username = getpass.getuser() 
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    email_status=''
    df.insert(0,'Username',username)
    
    
    tablename='SMB - Base Price - Category Addition - MiniBar'    
        
    flag='update'  
    df.insert(1,'table_name',tablename)
    col_tuple=("table_name","id","sequence_id",
             "Username",
             "BusinessCode",
             "Customer Group",
             
             "Market - Country",
        	 "Beam Category",
           "Document Item Currency",
           "Amount",
           "Currency")
    
    col_list=['table_name','id','sequence_id',"Username",
             "BusinessCode",
             "Customer_Group",
            
             "Market_Country",
        	 "Beam_Category",
           "Document_Item_Currency",
           "Amount",
           "Currency"]
    
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename)
  
    return {"status":"success"},200
   
   
  
         
@smb_app1.route('/download_baseprice_category_minibar',methods=['GET'])

def SMB_baseprice_catecory_minibar_download():
   
        now = datetime.now()
        try:
           
            df = pd.read_sql('''select * from "SMB"."SMB - Base Price - Category Addition - MiniBar" where "active"='1' order by sequence_id ''', con=con)
            
            df.drop(['Username','updated_on','active','aprover1','aprover2','aprover3'],axis=1,inplace=True)
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            
            file=download_path+t+'baseprice_addition_minibar.xlsx'
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
           
            
        except:
            return {"status":"failure"},500
    
    
# *******************************************************************************************************************************************************************
# incoterm exceptions

@smb_app1.route('/data_baseprice_incoterm',methods=['GET','POST'])

def SMB_data_baseprice_incoterm():
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
        query='''select * from "SMB"."SMB - Base Price - Incoterm Exceptions" where "active"='1' order by sequence_id OFFSET {} LIMIT {} '''.format(lowerLimit,upperLimit) 
        df=pd.read_sql(query,con=con )
        count=db.query('select count(*) from "SMB"."SMB - Base Price - Incoterm Exceptions" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

@smb_app1.route('/delete_record_baseprice_incoterm',methods=['POST','GET','DELETE'])
@token_required
def delete_record_baseprice_incoterm():  
    
    id_value=json.loads(request.data)['id']
    id_value.append(0)
    id_value=tuple(id_value)
    try:
        query='''UPDATE "SMB"."SMB - Base Price - Incoterm Exceptions" SET "active"=0 WHERE "id" in {} '''.format(id_value)
        result=db.insert(query)
        
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app1.route('/get_record_baseprice_incoterm',methods=['GET','POST'])     
@token_required  
def get_record_incoterm():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Base Price - Incoterm Exceptions" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app1.route('/update_record_baseprice_incoterm',methods=['POST'])
@token_required
def update_record_incoterm():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    Market_Country=(query_parameters['Market_Country'])
    Customer_Group=(query_parameters["Customer_Group"])
    Incoterm1=(query_parameters["Incoterm1"])
    Product_Division =( query_parameters["Product_Division"])
    Beam_Category=(query_parameters["Beam_Category"])
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    sequence_id=(query_parameters["sequence_id"])
    
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    id_value=(query_parameters['id_value'])
    flag='update'
      
    tablename='SMB - Base Price - Incoterm Exceptions'    
    input_tuple=(tablename,id_value,sequence_id,username,  Market_Country,Customer_Group,Incoterm1,Product_Division,Beam_Category,Delivering_Mill,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name","id","sequence_id",
         "Username",
             "Market - Country",
         "Customer Group",
         "Incoterm1", "Product Division", "Beam Category", "Delivering Mill",
         "Document Item Currency", "Amount", "Currency")
    email_status=''
    
    status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
    if(status['status']=='success'):email_status=email([status['tableid']],tablename)
    
    
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500



    
    
    
        
        
       

@smb_app1.route('/add_record_baseprice_incoterm',methods=['POST'])
@token_required
def add_record_incoterm():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    Market_Country=(query_parameters['Market_Country'])
    Customer_Group=(query_parameters["Customer_Group"])
    Incoterm1=(query_parameters["Incoterm1"])
    Product_Division =( query_parameters["Product_Division"])
    Beam_Category=(query_parameters["Beam_Category"])
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    flag='add'
   
    tablename='SMB - Base Price - Category Addition - MiniBar'    
       
    
    input_tuple=(tablename,flag,username,  Market_Country,Customer_Group,Incoterm1,Product_Division,Beam_Category,Delivering_Mill,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name","flag", "Username",
             "Market - Country",
         "Customer Group",
         "Incoterm1", "Product Division", "Beam Category", "Delivering Mill",
         "Document Item Currency", "Amount", "Currency")
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename)
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500


   
@smb_app1.route('/upload_baseprice_incoterm', methods=['GET','POST'])
@token_required
def  SMB_upload_baseprice_incoterm():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","Market - Country", "Customer Group",
       "Incoterm1", "Product Division", "Beam Category", "Delivering Mill",
       "Document Item Currency", "Amount", "Currency","sequence_id"]]  
            df["id"]=df["id"].astype(int)
            df["sequence_id"].astype(int)
            
            df_main = pd.read_sql('''select "id","Market - Country", "Customer Group",
       "Incoterm1", "Product Division", "Beam Category", "Delivering Mill",
       "Document Item Currency", "Amount", "Currency",sequence_id from "SMB"."SMB - Base Price - Incoterm Exceptions" where "active"='1' order by sequence_id ''', con=con)
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500
        
   

@smb_app1.route('/validate_baseprice_incoterm', methods=['GET','POST'])
@token_required
def  SMB_validate_baseprice_incoterm():
    json_data=json.loads(request.data)
    username = getpass.getuser() 
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    email_status=''
    df.insert(0,'Username',username)
    
    tablename='SMB - Base Price - Category Addition' 
    flag='update'  
    df.insert(1,'table_name',tablename)
    col_tuple=("table_name","id","sequence_id",
         "Username",
             "Market - Country",
         "Customer Group",
         "Incoterm1", "Product Division", "Beam Category", "Delivering Mill",
         "Document Item Currency", "Amount", "Currency")
   
    col_list=["table_name","id","sequence_id",
         "Username",
             "Market - Country",
         "Customer Group",
         "Incoterm1", "Product Division", "Beam Category", "Delivering Mill",
         "Document Item Currency", "Amount", "Currency"]
    
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename)
  
    return {"status":"success"},200
        
@smb_app1.route('/download_baseprice_incoterm',methods=['GET'])

def SMB_baseprice_download_incoterm():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select * from "SMB"."SMB - Base Price - Incoterm Exceptions" where "active"='1' order by sequence_id ''', con=con)
            df.drop(['Username','updated_on','active','aprover1','aprover2','aprover3'],axis=1,inplace=True)
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'extra_certificate.xlsx'
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
           
        except:
            return {"status":"failure"},500

# *****************************************************************************************************************************************
# smb extra certificate


@smb_app1.route('/data_extra_certificate',methods=['GET','POST'])
@token_required
def extra_certificate_data():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    
    if(limit==None):
        limit=500
    if(offset==None):
        offset=0
    
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    

    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Certificate" where "active"='1' order by sequence_id  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Certificate" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app1.route('/delete__record_extra_certificate',methods=['POST','GET','DELETE'])
@token_required
def delete_extra_certificate():  
    id_value=json.loads(request.data)['id']
    id_value.append(0)
    id_value=tuple(id_value)
    try:
        query='''UPDATE "SMB"."SMB - Extra - Certificate" SET "active"=0 WHERE "id" in {} '''.format(id_value)
        result=db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app1.route('/get_record_extra_certificate',methods=['GET','POST']) 
@token_required      
def get_record_extra_certificate():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Certificate" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app1.route('/update_record_extra_certificate',methods=['POST'])
@token_required
def add_record_extra_certificate():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Certificate=(query_parameters['Certificate'])
    Grade_Category =( query_parameters["Grade_Category"])
    Market_Country =( query_parameters["Market_Country"])
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    sequence_id=(query_parameters["sequence_id"])
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    id_value=(query_parameters['id'])
    
    try:
        
        query1='''INSERT INTO "SMB"."SMB - Extra - Certificate_History" 
        SELECT 
        "id","Username",now(),"BusinessCode", "Certificate",
       "Grade Category", "Market - Country", "Delivering Mill",
       "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Certificate"
        WHERE "id"={} '''.format(id_value)
        result=db.insert(query1)
        print(query1)
        if result=='failed' :raise ValueError
    
        query2='''UPDATE "SMB"."SMB - Extra - Certificate"
        SET 
       "Username"='{0}',
       "BusinessCode"='{1}',
       "Certificate"='{2}',
       "Grade Category"='{3}',
       "Market - Country"='{4}',
       "Delivering Mill"='{5}',
       "Document Item Currency"='{6}',
       "Amount"='{7}',
       "Currency"=''{8}'',
       "updated_on"='{9}',
       sequence_id={10}
        WHERE "id"={11} '''.format(username,BusinessCode,Certificate,Grade_Category,Market_Country,Delivering_Mill,Document_Item_Currency,Amount,Currency,date_time,sequence_id,id_value)
        result1=db.insert(query2)
        print(query2)
        if result1=='failed' :raise ValueError
        print(query1)
        print("*****************************")
        print(query2)
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
     
        
@smb_app1.route('/add_record_extra_certificate',methods=['GET','POST'])
@token_required
def smb_add_certificate():
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Certificate=(query_parameters['Certificate'])
    Grade_Category =( query_parameters["Grade_Category"])
    Market_Country =( query_parameters["Market_Country"])
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    
    
    try:
        input_tuple=(username, BusinessCode,Certificate,Grade_Category, Market_Country, Delivering_Mill,Document_Item_Currency, Amount,Currency.strip("'"))
        query='''INSERT INTO "SMB"."SMB - Extra - Certificate"(
             
             "Username",
             "BusinessCode", "Certificate",
       "Grade Category", "Market - Country", "Delivering Mill",
       "Document Item Currency", "Amount", "Currency")
             VALUES{};'''.format(input_tuple)
        result=db.insert(query)  
        if result=='failed' :raise ValueError
        
        return {"status":"success"},200
        
    except:
        return {"status":"failure"},500





   
@smb_app1.route('/upload_extra_certificate', methods=['GET','POST'])
@token_required
def  Upload_extra_certificate():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[['id',"BusinessCode", "Certificate",
       "Grade Category", "Market - Country", "Delivering Mill",
       "Document Item Currency", "Amount", "Currency","sequence_id"]]  
            df["id"]=df["id"].astype(int)
            df["sequence_id"]=df["sequence_id"].astype(int)
            
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Certificate",
       "Grade Category", "Market - Country", "Delivering Mill",
       "Document Item Currency", "Amount", "Currency",sequence_id from "SMB"."SMB - Extra - Certificate" where "active"='1' order by sequence_id ''', con=con)
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500


@smb_app1.route('/validate_extra_certificate', methods=['GET','POST'])
@token_required
def  validate_extra_certificate():
    
        
        json_data=json.loads(request.data)
        username = getpass.getuser() 
        now = datetime.now()
        date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
        
        df=pd.DataFrame(json_data["billet"]) 
        
        df.insert(0,'Username',username)
        df.insert(1,'date_time',date_time)
        
        try:
   
            df=df[ ["Username","BusinessCode", "Certificate",
           "Grade_Category", "Market_Country", "Delivering_Mill",
           "Document_Item_Currency", "Amount", "Currency","date_time","sequence_id","id"]]
            
            query1='''INSERT INTO "SMB"."SMB - Extra - Certificate_History" 
            SELECT 
            "id",
            "Username",now(),
            "BusinessCode", "Certificate",
           "Grade Category", "Market - Country", "Delivering Mill",
           "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Certificate"
            WHERE "id" in {} '''
            
            id_tuple=tuple(df["id"])
            if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
            result=db.insert(query1.format(id_tuple))
            if result=='failed' :raise ValueError
                
            # looping for update query
            for i in range(0,len(df)):
                print(tuple(df.loc[0]))
                query2='''UPDATE "SMB"."SMB - Extra - Certificate"
            SET 
           "Username"='%s',
           "BusinessCode"='%s',
           "Certificate"='%s',
           "Grade Category"='%s', "Market - Country"='%s', "Delivering Mill"='%s',
           "Document Item Currency"='%s',
           "Amount"='%s',
           "Currency"=''%s'',
           "updated_on"='%s',sequence_id='%s'
            WHERE "id"= '%s' ''' % tuple(df.loc[i])
                result=db.insert(query2)
                if result=='failed' :raise ValueError
                
            return {"status":"success"},200
        except:
            return {"status":"failure"},500
       
            
         
@smb_app1.route('/download_extra_certificate',methods=['GET'])

def download_extra_certificate():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select * from "SMB"."SMB - Extra - Certificate" where "active"='1' order by sequence_id ''', con=con)
            df.drop(['Username','updated_on','active','aprover1','aprover2','aprover3'],axis=1,inplace=True)
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'extra_certificate.xlsx'
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
           
        except:
            return {"status":"failure"},500


# *****************************************************************************************************************************************
# smb extra certificate minibar



@smb_app1.route('/data_extra_certificate_minibar',methods=['GET','POST'])
@token_required
def extra_certificate_data_minibar():
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
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Certificate - MiniBar" where "active"='1' order by sequence_id  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Certificate - MiniBar" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app1.route('/delete_record_extra_certificate_minibar',methods=['POST','GET','DELETE'])
@token_required
def delete_extra_certificate_minibar():  
    id_value=json.loads(request.data)['id']
    id_value.append(0)
    id_value=tuple(id_value)
    try:
        query='''UPDATE "SMB"."SMB - Extra - Certificate - MiniBar" SET "active"=0 WHERE "id" in {} '''.format(id_value)
        result=db.insert(query)
        

        if result=='failed': raise ValueError
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app1.route('/get_record_extra_certificate_minibar',methods=['GET','POST'])       
def get_record_extra_certificate_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Certificate - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app1.route('/update_record_extra_certificate_minibar',methods=['POST'])
@token_required
def update_record_extra_certificate_minibar():
    
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Customer_Group=(query_parameters['Customer_Group'])
   
    Market_Country =( query_parameters["Market_Country"])
    Certificate=(query_parameters['Certificate'])
    Grade_Category =( query_parameters["Grade_Category"])
    id_value=(query_parameters['id'])
    sequence_id=(query_parameters['sequence_id'])
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    try:
        
        query1='''INSERT INTO "SMB"."SMB - Extra - Certificate - MiniBar_History"
        SELECT 
        "id","Username",now(),"BusinessCode", "Customer Group",
       "Market - Customer", "Market - Country", "Certificate",
       "Grade Category",
       "Document Item Currency",
       "Amount",
       "Currency"  FROM "SMB"."SMB - Extra - Certificate - MiniBar"
        WHERE "id"={} '''.format(id_value)
        result=db.insert(query1)
        if result=='failed' :raise ValueError
    
        query2='''UPDATE "SMB"."SMB - Extra - Certificate - MiniBar"
        SET 
       "Username"='{0}',
       "BusinessCode"='{1}', "Customer Group"='{2}',
       "Market - Country"='{3}', "Certificate"='{4}',
       "Grade Category"='{5}',
       "Document Item Currency"='{6}',
       "Amount"='{7}',
       "Currency"=''{8}'',
       "updated_on"='{9}' , sequence_id={10}
        WHERE "id"={11} '''.format(username,BusinessCode,Customer_Group,Market_Country,Certificate,Grade_Category,Document_Item_Currency,Amount,Currency,date_time,sequence_id,id_value)
        result1=db.insert(query2)
        if result1=='failed' :raise ValueError
        print(query1)
        print("*****************************")
        print(query2)
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
     

@smb_app1.route('/add_record_extra_certificate_minibar',methods=['GET','POST'])
@token_required
def add_record_extra_certificate_minibar():
    
    
    username = getpass.getuser()
    
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Customer_Group=(query_parameters['Customer_Group'])
   
    Market_Country =( query_parameters["Market_Country"])
    Certificate=(query_parameters['Certificate'])
    Grade_Category =( query_parameters["Grade_Category"])
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    try:
        input_tuple=(username, BusinessCode, Customer_Group,Market_Customer,Market_Country,Certificate,Grade_Category, Document_Item_Currency, Amount,Currency)
        query='''INSERT INTO "SMB"."SMB - Extra - Certificate - MiniBar"(
             
             "Username",
             "BusinessCode", "Customer Group",
        "Market - Country", "Certificate",
       "Grade Category",
             "Document Item Currency",
             "Amount",
             "Currency")
             VALUES{};'''.format(input_tuple)
        result=db.insert(query)  
        if result=='failed' :raise ValueError
        
        return {"status":"success"},200
        
    except:
        return {"status":"failure"},500


   
@smb_app1.route('/upload_extra_certificate_minibar', methods=['GET','POST'])
@token_required
def  Upload_extra_certificate_minibar():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","BusinessCode", "Customer Group",
        "Market - Country", "Certificate",
       "Grade Category", "Document Item Currency", "Amount", "Currency","sequence_id"]]  
            df["id"]=df["id"].astype(int)
            df["sequence_id"]=df["sequence_id"].astype(int)
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Customer Group",
       "Market - Country", "Certificate",
       "Grade Category", "Document Item Currency", "Amount", "Currency",sequence_id from "SMB"."SMB - Extra - Certificate - MiniBar" where "active"='1' order by sequence_id ''', con=con)
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500



@smb_app1.route('/validate_extra_certificate_minibar', methods=['GET','POST'])
@token_required
def  validate_extra_certificate_minibar():
         json_data=json.loads(request.data)
         username = getpass.getuser() 
         now = datetime.now()
         date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
         
         df=pd.DataFrame(json_data["billet"]) 
         
         df.insert(0,'Username',username)
         df.insert(1,'date_time',date_time)
         try:

             df=df[ ["Username","BusinessCode", "Customer_Group",
       "Market_Country", "Certificate",
        "Grade_Category", "Document_Item_Currency", "Amount", "Currency","date_time","sequence_id","id"]]
             
             query1='''INSERT INTO "SMB"."SMB - Extra - Certificate - MiniBar_History" 
             SELECT 
             "id",
             "Username",now(),
             "BusinessCode", "Customer Group",
        "Market - Country", "Certificate",
        "Grade Category", "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Certificate - MiniBar"
             WHERE "id" in {} '''
             
             id_tuple=tuple(df["id"])
             if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
             result=db.insert(query1.format(id_tuple))
             if result=='failed' :raise ValueError
                     
             # looping for update query
             for i in range(0,len(df)):
                 print(tuple(df.loc[0]))
                 query2='''UPDATE "SMB"."SMB - Extra - Certificate - MiniBar"
             SET 
            "Username"='%s',
            "BusinessCode"='%s', "Customer Group"='%s',
        "Market - Country"='%s', "Certificate"='%s',
        "Grade Category"='%s', 
            "Document Item Currency"='%s',
            "Amount"='%s',
            "Currency"=''%s'',
            "updated_on"='%s',sequence_id='%s'
             WHERE "id"= '%s' ''' % tuple(df.loc[i])
                 result=db.insert(query2)
                 if result=='failed' :raise ValueError
                 
             return {"status":"success"},200
         except:
             return {"status":"failure"},500
            
        
         
@smb_app1.route('/download_extra_certificate_minibar',methods=['GET'])

def  download_extra_certificate_minibar():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select * from "SMB"."SMB - Extra - Certificate - MiniBar" where "active"='1' order by sequence_id ''', con=con)
            df.drop(['Username','updated_on','active','aprover1','aprover2','aprover3'],axis=1,inplace=True)
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'extra_certificate_minibar.xlsx'
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
           
        except:
            return {"status":"failure"},500


# *****************************************************************************************************************************************
# smb delivary mill


@smb_app1.route('/data_delivery_mill',methods=['GET','POST'])
def data_delivery_mill():
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
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Delivery Mill" where "active"='1' order by sequence_id OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Delivery Mill" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app1.route('/delete_record_delivery_mill',methods=['POST','GET','DELETE'])
@token_required
def delete_record_delivery_mill():  
    id_value=json.loads(request.data)['id']
    id_value.append(0)
    id_value=tuple(id_value)
    try:
        query='''UPDATE "SMB"."SMB - Extra - Delivery Mill" SET "active"=0 WHERE "id" in {} '''.format(id_value)
        result=db.insert(query)
        

        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app1.route('/get_record_delivery_mill',methods=['GET','POST']) 
@token_required      
def get_record_delivery_mill():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Delivery Mill" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app1.route('/update_record_delivery_mill',methods=['POST'])
@token_required
def update_record_delivery_mill():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Market_Country=(query_parameters['Market_Country'])
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    Product_Division =( query_parameters["Product_Division"])
    Beam_Category=(query_parameters["Beam_Category"])
   
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    id_value=(query_parameters['id'])
    sequence_id=(query_parameters['sequence_id'])
    try:
        
        query1='''INSERT INTO "SMB"."SMB - Extra - Delivery Mill_History"
        SELECT 
        "id","Username",now(),"BusinessCode", "Market - Country",
           "Delivering Mill", "Product Division", "Beam Category",
           "Document Item Currency", "Amount", "Currency" FROM "SMB"."SMB - Extra - Delivery Mill"
        WHERE "id"={} '''.format(id_value)
        result=db.insert(query1)
        print(query1)
        if result=='failed' :raise ValueError
    
        query2='''UPDATE "SMB"."SMB - Extra - Delivery Mill"
        SET 
       "Username"='{0}',
       "BusinessCode"='{1}', "Market - Country"='{2}',
       "Delivering Mill"='{3}', "Product Division"='{4}', "Beam Category"='{5}',
       
       
       "Document Item Currency"='{6}',
       "Amount"='{7}',
       "Currency"=''{8}'',
       "updated_on"='{9}',sequence_id={10}
        WHERE "id"={11} '''.format(username,BusinessCode,Market_Country,Delivering_Mill,Product_Division,Beam_Category,Document_Item_Currency,Amount,Currency,date_time,sequence_id,id_value)
        result1=db.insert(query2)
        print(query2)
        if result1=='failed' :raise ValueError
        print(query1)
        print("*****************************")
        print(query2)
        return {"status":"success"},200
    except:
        return {"status":"failure"},500

@smb_app1.route('/add_record_delivery_mill',methods=['POST'])
@token_required
def add_record_delivery_mill():
    
    
    username = getpass.getuser()
   
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Market_Country=(query_parameters['Market_Country'])
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    Product_Division =( query_parameters["Product_Division"])
    Beam_Category=(query_parameters["Beam_Category"])
   
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    
    try:
        input_tuple=(username, BusinessCode,  Market_Country,Delivering_Mill, Product_Division,Beam_Category,Document_Item_Currency, Amount,Currency.strip("'"))
        query='''INSERT INTO "SMB"."SMB - Extra - Delivery Mill"(
             
             "Username",
             "BusinessCode", "Market - Country",
       "Delivering Mill", "Product Division", "Beam Category",
       "Document Item Currency", "Amount", "Currency")
             VALUES{};'''.format(input_tuple)
        result=db.insert(query)  
        if result=='failed' :raise ValueError
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500

        
     

   

   
@smb_app1.route('/upload_delivery_mill', methods=['GET','POST'])
@token_required
def upload_delivery_mill():
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","BusinessCode", "Market - Country",
       "Delivering Mill", "Product Division", "Beam Category",
       "Document Item Currency", "Amount", "Currency","sequence_id"]]  
            df["id"]=df["id"].astype(int)
            df["sequence_id"]=df["sequence_id"].astype(int)
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Market - Country",
       "Delivering Mill", "Product Division", "Beam Category",
       "Document Item Currency", "Amount", "Currency",sequence_id from "SMB"."SMB - Extra - Delivery Mill" where "active"='1' order by sequence_id ''', con=con)
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500


    
   


@smb_app1.route('/validate_delivery_mill', methods=['GET','POST'])
@token_required
def  validate_delivery_mill():
    
        
    json_data=json.loads(request.data)
    username = getpass.getuser() 
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    df.insert(1,'date_time',date_time)
    try:
       
        df=df[ ["Username","BusinessCode", "Market_Country",
       "Delivering_Mill", "Product_Division", "Beam_Category",
       "Document_Item_Currency", "Amount", "Currency","date_time","sequence_id","id"]]
        
        query1='''INSERT INTO "SMB"."SMB - Extra - Delivery Mill_History"
        SELECT 
        "id",
        "Username",now(),
        "BusinessCode", "Market - Country",
       "Delivering Mill", "Product Division", "Beam Category",
       "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Delivery Mill"
        WHERE "id" in {} '''
        
        id_tuple=tuple(df["id"])
        if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
        result=db.insert(query1.format(id_tuple))
        if result=='failed' :raise ValueError
        
        # looping for update query
        for i in range(0,len(df)):
            print(tuple(df.loc[0]))
            query2='''UPDATE "SMB"."SMB - Extra - Delivery Mill"
        SET 
       "Username"='%s',
       "BusinessCode"='%s', "Market - Country"='%s',
       "Delivering Mill"='%s', "Product Division"='%s', "Beam Category"='%s',
       
       "Document Item Currency"='%s',
       "Amount"='%s',
       "Currency"=''%s'',
       "updated_on"='%s',sequence_id='%s'
        WHERE "id"= '%s' ''' % tuple(df.loc[i])
            result=db.insert(query2)
            if result=='failed' :raise ValueError
            
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
         
@smb_app1.route('/download_delivery_mill',methods=['GET'])
def  download_delivery_mill():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select * from "SMB"."SMB - Extra - Delivery Mill" where "active"='1' order by sequence_id ''', con=con)
            df.drop(['Username','updated_on','active','aprover1','aprover2','aprover3'],axis=1,inplace=True)
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'delivery_mill.xlsx'
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
           
        except:
            return {"status":"failure"},500
    
    

# *****************************************************************************************************************************************
# smb delivary mill minibar




@smb_app1.route('/data_delivery_mill_minibar',methods=['GET','POST'])
@token_required
def data_delivery_mill_minibar():
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
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Delivery Mill - MiniBar" where "active"='1' order by sequence_id OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Delivery Mill - MiniBar" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app1.route('/delete_record_delivery_mill_minibar',methods=['POST','GET','DELETE'])
@token_required
def delete_record_delivery_mill_minibar():  
    id_value=json.loads(request.data)['id']
    id_value.append(0)
    id_value=tuple(id_value)
    try:
        query='''UPDATE "SMB"."SMB - Extra - Delivery Mill - MiniBar" SET "active"=0 WHERE "id" in {} '''.format(id_value)
        result=db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app1.route('/get_record_delivery_mill_minibar',methods=['GET','POST'])       
def get_record_delivery_mill_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Delivery Mill - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app1.route('/update_record_delivery_mill_minibar',methods=['POST'])
@token_required
def update_record_delivery_mill_minibar():
    
        today = date.today()
        username = getpass.getuser()
        now = datetime.now()
        date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
        query_parameters =json.loads(request.data)
        
        
        Market_Country=(query_parameters['Market_Country'])
        Market_Customer_Group=(query_parameters['Market_Customer_Group'])
       
        Delivering_Mill=(query_parameters["Delivering_Mill"])
        Product_Division =( query_parameters["Product_Division"])
        id_value=(query_parameters['id'])
        sequence_id=(query_parameters['sequence_id'])
        
        Document_Item_Currency =( query_parameters["Document_Item_Currency"])
        Amount =( query_parameters["Amount"])
        Currency =( query_parameters["Currency"])

   
        try:
            query1='''INSERT INTO "SMB"."SMB - Extra - Delivery Mill - MiniBar_History"
            SELECT 
            "id","Username",now(),"Market - Country",
           "Market - Customer Group", "Market - Customer", "Delivering Mill",
           "Product Division", "Document Item Currency", "Amount", "Currency" FROM "SMB"."SMB - Extra - Delivery Mill - MiniBar"
            WHERE "id"={} '''.format(id_value)
            result=db.insert(query1)
            print(query1)
            if result=='failed' :raise ValueError
        
            query2='''UPDATE "SMB"."SMB - Extra - Delivery Mill - MiniBar"
            SET 
           "Username"='{0}',
           "Market - Country"='{1}',
           "Market - Customer Group"='{2}',"Delivering Mill"='{3}',
           "Product Division"='{4}', 
           "Document Item Currency"='{5}',
           "Amount"='{6}',
           "Currency"=''{7}'',
           "updated_on"='{8}', sequence_id={9}
            WHERE "id"={10} '''.format(username,Market_Country,Market_Customer_Group,Delivering_Mill,Product_Division,Document_Item_Currency,Amount,Currency,date_time,sequence_id,id_value)
            result1=db.insert(query2)
            print(query2)
            if result1=='failed' :raise ValueError
            
            return {"status":"success"},200
        except:
            return {"status":"failure"}
    
@smb_app1.route('/add_record_delivery_mill_minibar',methods=['POST'])
@token_required
def add_record_delivery_mill_minibar():
    
    username = getpass.getuser()
    
    query_parameters =json.loads(request.data)
    
    
    Market_Country=(query_parameters['Market_Country'])
    Market_Customer_Group=(query_parameters['Market_Customer_Group'])
  
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    Product_Division =( query_parameters["Product_Division"])
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    try:
        input_tuple=(username,   Market_Country,Market_Customer_Group,Delivering_Mill, Product_Division,Document_Item_Currency, Amount,Currency)
        query='''INSERT INTO "SMB"."SMB - Extra - Delivery Mill - MiniBar"(
             
             "Username",
             "Market - Country",
       "Market - Customer Group",  "Delivering Mill",
       "Product Division", "Document Item Currency", "Amount", "Currency")
             VALUES{};'''.format(input_tuple)
             
        result=db.insert(query)  
        print(query)
        if result=='failed' :raise ValueError
        
        return {"status":"success"},200
        
    except:
        return {"status":"failure"},500


    
   
@smb_app1.route('/upload_delivery_mill_minibar', methods=['GET','POST'])
@token_required
def upload_delivery_mill_minibar():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","Market - Country",
       "Market - Customer Group",  "Delivering Mill",
       "Product Division", "Document Item Currency", "Amount", "Currency","sequence_id"]]  
            df["id"]=df["id"].astype(int)
            df["sequence_id"]=df["sequence_id"].astype(int)
            
            df_main = pd.read_sql('''select "id","Market - Country",
       "Market - Customer Group", "Delivering Mill",
       "Product Division", "Document Item Currency", "Amount", "Currency","sequence_id" from "SMB"."SMB - Extra - Delivery Mill - MiniBar" where "active"='1' order by sequence_id ''', con=con)
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer","Zip_Code_(Dest)":"Zip_Code_Dest"},inplace=True)  
                
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500


@smb_app1.route('/validate_delivery_mill_minibar', methods=['GET','POST'])
@token_required
def  validate_delivery_mill_minibar():
    
        
            json_data=json.loads(request.data)
            username = getpass.getuser() 
            now = datetime.now()
            date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
            
            df=pd.DataFrame(json_data["billet"]) 
            
            df.insert(0,'Username',username)
            df.insert(1,'date_time',date_time)
            
            try:
    
       
                df=df[ ["Username","Market_Country",
               "Market_Customer_Group", "Delivering_Mill",
               "Product_Division", "Document_Item_Currency", "Amount", "Currency","date_time","sequence_id","id"]]
                
                query1='''INSERT INTO "SMB"."SMB - Extra - Delivery Mill - MiniBar_History"
                SELECT 
                "id",
                "Username",now(),
                "Market - Country",
               "Market - Customer Group", "Delivering Mill",
               "Product Division", "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Delivery Mill - MiniBar"
                WHERE "id" in {} '''
                
                id_tuple=tuple(df["id"])
                if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
                result=db.insert(query1.format(id_tuple))
                if result=='failed' :raise ValueError
                        
                # looping for update query
                for i in range(0,len(df)):
                    print(tuple(df.loc[0]))
                    query2='''UPDATE "SMB"."SMB - Extra - Delivery Mill - MiniBar"
                SET 
               "Username"='%s',
               "Market - Country"='%s', "Market - Customer Group"='%s',
               "Market - Customer"='%s', "Delivering Mill"='%s', "Product Division"='%s',
               
               "Document Item Currency"='%s',
               "Amount"='%s',
               "Currency"=''%s'',
               "updated_on"='%s',sequence_id='%s'
                WHERE "id"= '%s' ''' % tuple(df.loc[i])
                    result=db.insert(query2)
                    print(query2)
                    if result=='failed' :raise ValueError
                    
                return {"status":"success"},200
            except:
                return {"status":"failure"},500        
           
@smb_app1.route('/download_delivery_mill_minibar',methods=['GET'])

def  download_delivery_mill_minibar():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select * from "SMB"."SMB - Extra - Delivery Mill - MiniBar" where "active"='1' order by sequence_id ''', con=con)
            df.drop(['Username','updated_on','active','aprover1','aprover2','aprover3'],axis=1,inplace=True)
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'delivery_mill_minibar.xlsx'
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
           
        except:
            return {"status":"failure"},500
    
        
    
@smb_app1.route('/history_delivering_mill',methods=['GET'])

def  download_delivery_mill_minibar_history():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Delivery Mill - MiniBar_History"   OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Delivery Mill - MiniBar"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

    
   


