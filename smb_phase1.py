# -*- coding: utf-8 -*-
"""
Created on Thu Oct  12 07:33:38 2021

@author: subbu

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
               
 
smb_app1 = Blueprint('smb_app1', __name__)

CORS(smb_app1)

db=Database()

input_directory="C:/Users/Administrator/Documents/SMB_INPUT/"


download_path='/home/ubuntu/SMBDir/smb_download/'

con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')



@smb_app1.route('/hello_world',methods=['GET'])
def Hello():
    return {"status":"success"}


@smb_app1.route('/Base_Price_Data',methods=['GET','POST'])
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
        query='''select * from "SMB"."SMB - Base Price - Category Addition" where "active"='1' order by "id"  OFFSET {} LIMIT {} '''.format(lowerLimit,upperLimit) 
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
def delete_record():  
    id_value=request.args.get('id')
    
    try:
        query='''UPDATE "SMB"."SMB - Base Price - Category Addition" SET "active"=0 WHERE "id"={} '''.format(id_value)
        result=db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app1.route('/get_record_baseprice',methods=['GET','POST'])       
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
        
        
    
        
        query1='''INSERT INTO "SMB"."SMB - Base Price - Category Addition_History" 
        SELECT 
        "id","Username","BusinessCode","Market - Country",
      	   "Product Division",
       "Product Level 02",
       "Document Item Currency",
       "Amount",
       "Currency"  FROM "SMB"."SMB - Base Price - Category Addition"
        WHERE "id"={} '''.format(id_value)
        result=db.insert(query1)
        if result=='failed' :raise ValueError
    
        query2='''UPDATE "SMB"."SMB - Base Price - Category Addition"
        SET 
       "Username"='{0}',
       "BusinessCode"='{1}',
       "Market - Country"='{2}',
      	   "Product Division"='{3}',
       "Product Level 02"='{4}',
       "Document Item Currency"='{5}',
       "Amount"='{6}',
       "Currency"=''{7}'',
       "updated_on"='{8}',
        WHERE "id"={9} '''.format(username,BusinessCode,Market_Country,Product_Division,Product_Level_02,Document_Item_Currency,Amount,Currency,date_time,id_value)
        result1=db.insert(query2)
        if result1=='failed' :raise ValueError
        return {"status":"success"},200
   

@smb_app1.route('/add_record_baseprice',methods=['POST'])
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
    
    
    try:
        input_tuple=(username, BusinessCode,  Market_Country, Product_Division,Product_Level_02,Document_Item_Currency, Amount,Currency.strip("'"))
        query='''INSERT INTO "SMB"."SMB - Base Price - Category Addition"(
             
             "Username",
             "BusinessCode",
             "Market - Country",
             "Product Division",
             "Product Level 02",
             "Document Item Currency",
             "Amount",
             "Currency")
             VALUES{};'''.format(input_tuple)
        result=db.insert(query)  
        if result=='failed' :raise ValueError
        
        return {"status":"success"},200
        
    except:
        return {"status":"failure"},500




   
@smb_app1.route('/Base_Price_Upload', methods=['GET','POST'])
def  SMB_upload():
    
            f=request.files['filename']
      
                
            f.save(input_directory+f.filename)
            
       
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[['id','BusinessCode','Market - Country','Product Division','Product Level 02','Document Item Currency', 'Amount', 'Currency']]  
            df['id']=df['id'].astype(int)
            
            df_main = pd.read_sql('''select "id","BusinessCode","Market - Country","Product Division","Product Level 02","Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Base Price - Category Addition" where "active"='1' order by "id" ''', con=con)
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
       

@smb_app1.route('/Base_Price_validate', methods=['GET','POST'])
def  SMB_validate():
    json_data=json.loads(request.data)
    username = getpass.getuser() 
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    df.insert(1,'date_time',date_time)
    
    try:
        df=df[ ['Username','BusinessCode','Market_Country','Product_Division','Product_Level_02','Document_Item_Currency', 'Amount', 'Currency','date_time','id']]
        
        query1='''INSERT INTO "SMB"."SMB - Base Price - Category Addition_History" 
        SELECT 
        "id",
        "Username",
        "BusinessCode",
        "Market - Country",
    	"Product Division",
       "Product Level 02",
       "Document Item Currency",
       "Amount",
       "Currency"  FROM "SMB"."SMB - Base Price - Category Addition"
        WHERE "id" in {} '''
        
        id_tuple=tuple(df["id"])
        if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
        result=db.insert(query1.format(id_tuple))
        if result=='failed' :raise ValueError
        
        # looping for update query
        for i in range(0,len(df)):
           
            query2='''UPDATE "SMB"."SMB - Base Price - Category Addition"
        SET 
       "Username"='%s',
       "BusinessCode"='%s',
       "Market - Country"='%s',
       "Product Division"='%s',
       "Product Level 02"='%s',
       "Document Item Currency"='%s',
       "Amount"='%s',
       "Currency"=''%s'',
       "updated_on"='%s'
        WHERE "id"= '%s' ''' % tuple(df.loc[i])
            result=db.insert(query2)
            if result=='failed' :raise ValueError
            
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
        
  
         
@smb_app1.route('/Base_price_download',methods=['GET'])
def SMB_baseprice_download1():
   
        now = datetime.now()
       
        df = pd.read_sql('''select * from "SMB"."SMB - Base Price - Category Addition" where "active"='1' order by sequence_id ''', con=con)
        df.drop(['Username','updated_on','active','aprover1','aprover2','aprover3'],axis=1,inplace=True)
        t=now.strftime("%d-%m-%Y-%H-%M-%S")
        file=download_path+t+'baseprice_category_addition.xlsx'
        print(file)
        df.to_excel(file,index=False)
        
        return send_file(file, as_attachment=True)
       
        
    
# ***************************************************************************************************************************************************************************************
# baseprice_minibar

@smb_app1.route('/data_baseprice_category_minibar',methods=['GET','POST'])
def SMB_data_baseprice_mini():
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
def delete_record_baseprice_mini():  
    id_value=request.args.get('id')
    try:
        query='''UPDATE "SMB"."SMB - Base Price - Category Addition - MiniBar" SET "active"=0 WHERE "id"={} '''.format(id_value)
        result=db.insert(query)
        
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app1.route('/get_record_baseprice_category_minibar',methods=['GET','POST'])       
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
def update_record_category_minibar():
        username = getpass.getuser()
        now = datetime.now()
        date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
        query_parameters =json.loads(request.data) 
        
        BusinessCode=(query_parameters['BusinessCode'])
        Customer_Group=(query_parameters["Customer_Group"])
        Market_Customer=(query_parameters["Market_Customer"])
        Market_Country =( query_parameters["Market_Country"])
        Beam_Category=(query_parameters["Beam_Category"])
        
        
        
        Document_Item_Currency =( query_parameters["Document_Item_Currency"])
        Amount =( query_parameters["Amount"])
        Currency =( query_parameters["Currency"])
        sequence_id=(query_parameters['sequence_id'])
        id_value=(query_parameters['id_value'])
    
        try:
        
            query1='''INSERT INTO "SMB"."SMB - Base Price - Category Addition - MiniBar_History" 
            SELECT 
            "id","Username",now(),"BusinessCode",
             "Customer Group",
             "Market - Customer",
             "Market - Country",
        	 "Beam Category",
           "Document Item Currency",
           "Amount",
           "Currency"  FROM "SMB"."SMB - Base Price - Category Addition - MiniBar"
            WHERE "id"={} '''.format(id_value)
            result=db.insert(query1)
            if result=='failed' :raise ValueError
        
            query2='''UPDATE "SMB"."SMB - Base Price - Category Addition - MiniBar"
            SET 
           "Username"='{0}',
           "BusinessCode"='{1}',
           "Customer Group"='{2}',
          	   "Market - Customer"='{3}',
           "Market - Country"='{4}',
           "Beam Category"='{5}',
           "Document Item Currency"='{6}',
           "Amount"='{7}',
           "Currency"=''{8}'',
           "updated_on"='{9}',sequence_id= {10}
            WHERE "id"={11} '''.format(username,BusinessCode,Customer_Group,Market_Customer,Market_Country,Beam_Category,Document_Item_Currency,Amount,Currency,date_time,sequence_id,id_value)
            print(query2)
            
            result1=db.insert(query2)
            if result1=='failed' :raise ValueError
            
            return {"status":"success"},200
        except: {"status":"failure"},500
   


@smb_app1.route('/add_record_baseprice_category_minibar',methods=['POST'])
def add_record_mini():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Customer_Group=(query_parameters["Customer_Group"])
    Market_Customer=(query_parameters["Market_Customer"])
    Market_Country =( query_parameters["Market_Country"])
    Beam_Category=(query_parameters["Beam_Category"])
    
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    
  
    try:
    
        input_tuple=( username, BusinessCode, Customer_Group, Market_Customer, Market_Country, Beam_Category,Document_Item_Currency, Amount,Currency.strip("'"))
        
        query='''INSERT INTO "SMB"."SMB - Base Price - Category Addition - MiniBar"(
             
             "Username",
             
             "BusinessCode",
             "Customer Group",
             "Market - Customer",
             "Market - Country",
        	 "Beam Category",
             "Document Item Currency",
             "Amount",
             "Currency")
             VALUES{};'''.format(input_tuple)
        print(query)
             
       
        result=db.insert(query)
        if result=='failed' :raise ValueError
            
        return {"status":"success"},200
    except:
        return {"status":"failure"},500

 
   
@smb_app1.route('/upload_baseprice_category_minibar', methods=['GET','POST'])
def  SMB_upload_baseprice_minibar():
    
            f=request.files['filename']
      
                
            f.save(input_directory+f.filename)
        
        
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[['id','BusinessCode', 'Customer Group','Market - Customer', 'Market - Country', 'Beam Category','Document Item Currency', 'Amount', 'Currency']]  
            df['id']=df['id'].astype(int)
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Customer Group","Market - Customer", "Market - Country", "Beam Category","Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Base Price - Category Addition - MiniBar" where "active"='1' order by "id" ''', con=con)
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
       


@smb_app1.route('/validate_baseprice_category_minibar', methods=['GET','POST'])
def  SMB_validate_baseprice_minibar():
    json_data=json.loads(request.data)
    username = getpass.getuser() 
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    df.insert(1,'date_time',date_time)
    
    try:
    
        df=df[ ["Username","BusinessCode", "Customer_Group",
       "Market_Customer", "Market_Country", "Beam_Category",
       "Document_Item_Currency", "Amount", "Currency","date_time","id"]]
        
        query1='''INSERT INTO "SMB"."SMB - Base Price - Category Addition - MiniBar_History" 
        SELECT 
        "id",
        "Username",now(),
        "BusinessCode", "Customer Group","Market - Customer", "Market - Country", "Beam Category","Document Item Currency", "Amount", "Currency" FROM "SMB"."SMB - Base Price - Category Addition - MiniBar"
        WHERE "id" in {} '''
        
        id_tuple=tuple(df["id"])
        if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
        result=db.insert(query1.format(id_tuple))
        print(query1.format(id_tuple))
        if result=='failed' :raise ValueError
        
        
        # looping for update query
        for i in range(0,len(df)):
           
            query2='''UPDATE "SMB"."SMB - Base Price - Category Addition - MiniBar"
        SET 
       "Username"='%s',
       "BusinessCode"='%s', "Customer Group"='%s',"Market - Customer"='%s', "Market - Country"='%s', "Beam Category"='%s',"Document Item Currency"='%s', "Amount"='%s', "Currency"=''%s'',
       "updated_on"='%s'
        WHERE "id"= '%s' ''' % tuple(df.loc[i])
            result=db.insert(query2)
            print(query2)
            if result=='failed' :raise ValueError
            
        return {"status":"success"},200
    except: return {"status":"failure"},500

  
         
@smb_app1.route('/download_baseprice_category_minibar',methods=['GET'])
def SMB_baseprice_catecory_minibar_download():
   
        now = datetime.now()
        try:
            print("hi")
            df = pd.read_sql('''select * from "SMB"."SMB - Base Price - Category Addition - MiniBar" where "active"='1' order by "id" ''', con=con)
            
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
        query='''select * from "SMB"."SMB - Base Price - Incoterm Exceptions" where "active"='1' order by "id"  OFFSET {} LIMIT {} '''.format(lowerLimit,upperLimit) 
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
def delete_record_baseprice_incoterm():  
    id_value=request.args.get('id')
    try:
        query='''UPDATE "SMB"."SMB - Base Price - Incoterm Exceptions" SET "active"=0 WHERE "id"={} '''.format(id_value)
        result=db.insert(query)
        
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app1.route('/get_record_baseprice_incoterm',methods=['GET','POST'])       
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
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    id_value=request.args.get('id')
    
    
  
   
    try:
         query1='''INSERT INTO "SMB"."SMB - Base Price - Incoterm Exceptions_History" 
         SELECT 
         "id","Username",now(),
         "Market - Country",
         "Customer Group",
         "Incoterm1", "Product Division", "Beam Category", "Delivering Mill",
         "Document Item Currency", "Amount", "Currency" FROM "SMB"."SMB - Base Price - Incoterm Exceptions"
         WHERE "id"={} '''.format(id_value)
         result=db.insert(query1)
         if result=='failed' :raise ValueError
     
         query2='''UPDATE "SMB"."SMB - Base Price - Incoterm Exceptions"
         SET 
        "Username"='{0}',
        "Market - Country"='{1}', 
        "Customer Group"='{2}',
        "Incoterm1"='{3}',
        "Product Division"='{4}',
        "Beam Category"='{5}', 
        "Delivering Mill"='{6}',
        "Document Item Currency"='{7}',
        "Amount"='{8}', 
        "Currency"=''{9}'',
        "updated_on"='{10}'
         WHERE "id"={11} '''.format(username,Market_Country,Customer_Group,Incoterm1,Product_Division,Beam_Category,Delivering_Mill,Document_Item_Currency,Amount,Currency,date_time,id_value)
         
         result1=db.insert(query2)
         if result1=='failed' :raise ValueError
         
         return {"status":"success"},200
    except:
         return {"status":"failure"},500
     
        
       
       

@smb_app1.route('/add_record_baseprice_incoterm',methods=['POST'])
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
    
    print(Currency)
    
    
    try:
        input_tuple=(username,  Market_Country,Customer_Group,Incoterm1,Product_Division,Beam_Category,Delivering_Mill,Document_Item_Currency,Amount,Currency.strip("'"))
        query='''INSERT INTO "SMB"."SMB - Base Price - Incoterm Exceptions"(
             
             "Username",
             "Market - Country",
         "Customer Group",
         "Incoterm1", "Product Division", "Beam Category", "Delivering Mill",
         "Document Item Currency", "Amount", "Currency")
             VALUES{};'''.format(input_tuple)
        result=db.insert(query) 
        print(query)
        if result=='failed' :raise ValueError
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
        
    


   
@smb_app1.route('/upload_baseprice_incoterm', methods=['GET','POST'])
def  SMB_upload_baseprice_incoterm():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","Market - Country", "Customer Group",
       "Incoterm1", "Product Division", "Beam Category", "Delivering Mill",
       "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            df_main = pd.read_sql('''select "id","Market - Country", "Customer Group",
       "Incoterm1", "Product Division", "Beam Category", "Delivering Mill",
       "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Base Price - Incoterm Exceptions" where "active"='1' order by "id" ''', con=con)
            
            
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
def  SMB_validate_baseprice_incoterm():
        json_data=json.loads(request.data)
        username = getpass.getuser() 
        now = datetime.now()
        date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
        
        df=pd.DataFrame(json_data["billet"]) 
        
        df.insert(0,'Username',username)
        df.insert(1,'date_time',date_time)
    
   
    
        df=df[ ["Username","Market_Country", "Customer_Group",
       "Incoterm1", "Product_Division", "Beam_Category", "Delivering_Mill",
       "Document_Item_Currency", "Amount", "Currency","date_time","id"]]
        
        query1='''INSERT INTO "SMB"."SMB - Base Price - Incoterm Exceptions_History" 
        SELECT 
        "id",
        "Username",now(),
        "Market - Country", "Customer Group",
       "Incoterm1", "Product Division", "Beam Category", "Delivering Mill",
       "Document Item Currency", "Amount", "Currency" FROM "SMB"."SMB - Base Price - Incoterm Exceptions"
        WHERE "id" in {} '''
        
        id_tuple=tuple(df["id"])
        if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
        result=db.insert(query1.format(id_tuple))
        print(query1.format(id_tuple))
        if result=='failed' :raise ValueError
        
        
        # looping for update query
        for i in range(0,len(df)):
           
            query2='''UPDATE "SMB"."SMB - Base Price - Incoterm Exceptions"
        SET 
       "Username"='%s',
       "Market - Country"='%s', "Customer Group"='%s',
       "Incoterm1"='%s', "Product Division"='%s', "Beam Category"='%s', "Delivering Mill"='%s',
       "Document Item Currency"='%s', "Amount"='%s', "Currency"=''%s'',
       "updated_on"='%s'
        WHERE "id"= '%s' ''' % tuple(df.loc[i])
            result=db.insert(query2)
            print(query2)
            if result=='failed' :raise ValueError
            
        return {"status":"success"},200
   
  
        
@smb_app1.route('/download_baseprice_incoterm',methods=['GET'])
def SMB_baseprice_download_incoterm():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select * from "SMB"."SMB - Base Price - Incoterm Exceptions" where "active"='1' order by "id" ''', con=con)
            df.drop(['Username','updated_on','active','aprover1','aprover2','aprover3'],axis=1,inplace=True)
            df.to_excel('C:/Users/Administrator/Downloads/'+now.strftime("%d-%m-%Y-%H-%M-%S") +'baseprice_incoterm_exceptions.xlsx',index=False)
            return {"status":"success"},200
        except:
            return {"status":"failure"},500



# *****************************************************************************************************************************************
# smb extra certificate


@smb_app1.route('/data_extra_certificate',methods=['GET','POST'])
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
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Certificate" where "active"='1' order by "id"  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Certificate"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app1.route('/delete__record_extra_certificate',methods=['POST','GET','DELETE'])
def delete_extra_certificate():  
    id_value=request.args.get('id')
    try:
        query='''UPDATE "SMB"."SMB - Extra - Certificate" SET "active"=0 WHERE "id"={} '''.format(id_value)
        result=db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app1.route('/get_record_extra_certificate',methods=['GET','POST'])       
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
       "updated_on"='{9}'
        WHERE "id"={10} '''.format(username,BusinessCode,Certificate,Grade_Category,Market_Country,Delivering_Mill,Document_Item_Currency,Amount,Currency,date_time,id_value)
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
def  Upload_extra_certificate():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[['id',"BusinessCode", "Certificate",
       "Grade Category", "Market - Country", "Delivering Mill",
       "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Certificate",
       "Grade Category", "Market - Country", "Delivering Mill",
       "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Certificate" where "active"='1' order by "id" ''', con=con)
            
            
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
           "Document_Item_Currency", "Amount", "Currency","date_time","id"]]
            
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
           "updated_on"='%s'
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
            df = pd.read_sql('''select * from "SMB"."SMB - Extra - Certificate" where "active"='1' order by "id" ''', con=con)
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
        count=db.query('select count(*) from "SMB"."SMB - Extra - Certificate - MiniBar"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app1.route('/delete_record_extra_certificate_minibar',methods=['POST','GET','DELETE'])
def delete_extra_certificate_minibar():  
    id_value=request.args.get('id')
    try:
        query='''UPDATE "SMB"."SMB - Extra - Certificate - MiniBar" SET "active"=0 WHERE "id"={} '''.format(id_value)
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
def update_record_extra_certificate_minibar():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Customer_Group=(query_parameters['Customer_Group'])
    Market_Customer=(query_parameters['Market_Customer'])
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
       "Market - Customer"='{3}', "Market - Country"='{4}', "Certificate"='{5}',
       "Grade Category"='{6}',
       "Document Item Currency"='{7}',
       "Amount"='{8}',
       "Currency"=''{9}'',
       "updated_on"='{10}' , sequence_id={11}
        WHERE "id"={12} '''.format(username,BusinessCode,Customer_Group,Market_Customer,Market_Country,Certificate,Grade_Category,Document_Item_Currency,Amount,Currency,date_time,sequence_id,id_value)
        result1=db.insert(query2)
        if result1=='failed' :raise ValueError
        print(query1)
        print("*****************************")
        print(query2)
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
     

@smb_app1.route('/add_record_extra_certificate_minibar',methods=['GET','POST'])
def add_record_extra_certificate_minibar():
    
    
    username = getpass.getuser()
    
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Customer_Group=(query_parameters['Customer_Group'])
    Market_Customer=(query_parameters['Market_Customer'])
    Market_Country =( query_parameters["Market_Country"])
    Certificate=(query_parameters['Certificate'])
    Grade_Category =( query_parameters["Grade_Category"])
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    try:
        input_tuple=(username, BusinessCode, Customer_Group,Market_Customer,Market_Country,Certificate,Grade_Category, Document_Item_Currency, Amount,Currency.strip("'"))
        query='''INSERT INTO "SMB"."SMB - Extra - Certificate - MiniBar"(
             
             "Username",
             "BusinessCode", "Customer Group",
       "Market - Customer", "Market - Country", "Certificate",
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
def  Upload_extra_certificate_minibar():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","BusinessCode", "Customer Group",
       "Market - Customer", "Market - Country", "Certificate",
       "Grade Category", "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Customer Group",
       "Market - Customer", "Market - Country", "Certificate",
       "Grade Category", "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Certificate - MiniBar" where "active"='1' order by "id" ''', con=con)
            
            
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
        "Market_Customer", "Market_Country", "Certificate",
        "Grade_Category", "Document_Item_Currency", "Amount", "Currency","date_time","id"]]
             
             query1='''INSERT INTO "SMB"."SMB - Extra - Certificate - MiniBar_History" 
             SELECT 
             "id",
             "Username",now(),
             "BusinessCode", "Customer Group",
        "Market - Customer", "Market - Country", "Certificate",
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
        "Market - Customer"='%s', "Market - Country"='%s', "Certificate"='%s',
        "Grade Category"='%s', 
            "Document Item Currency"='%s',
            "Amount"='%s',
            "Currency"=''%s'',
            "updated_on"='%s'
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
            df = pd.read_sql('''select * from "SMB"."SMB - Extra - Certificate - MiniBar" where "active"='1' order by "id" ''', con=con)
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
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Delivery Mill" where "active"='1' order by "id"  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Delivery Mill"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app1.route('/delete_record_delivery_mill',methods=['POST','GET','DELETE'])
def delete_record_delivery_mill():  
    id_value=request.args.get('id')
    try:
        query='''UPDATE "SMB"."SMB - Extra - Delivery Mill" SET "active"=0 WHERE "id"={} '''.format(id_value)
        result=db.insert(query)
        

        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app1.route('/get_record_delivery_mill',methods=['GET','POST'])       
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
       "updated_on"='{9}'
        WHERE "id"={10} '''.format(username,BusinessCode,Market_Country,Delivering_Mill,Product_Division,Beam_Category,Document_Item_Currency,Amount,Currency,date_time,id_value)
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
def upload_delivery_mill():
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","BusinessCode", "Market - Country",
       "Delivering Mill", "Product Division", "Beam Category",
       "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Market - Country",
       "Delivering Mill", "Product Division", "Beam Category",
       "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Delivery Mill" where "active"='1' order by "id" ''', con=con)
            
            
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
       "Document_Item_Currency", "Amount", "Currency","date_time","id"]]
        
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
       "updated_on"='%s'
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
            df = pd.read_sql('''select * from "SMB"."SMB - Extra - Delivery Mill" where "active"='1' order by "id" ''', con=con)
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
        count=db.query('select count(*) from "SMB"."SMB - Extra - Delivery Mill - MiniBar"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app1.route('/delete_record_delivery_mill_minibar',methods=['POST','GET','DELETE'])
def delete_record_delivery_mill_minibar():  
    id_value=request.args.get('id')
    try:
        query='''UPDATE "SMB"."SMB - Extra - Delivery Mill - MiniBar" SET "active"=0 WHERE "id"={} '''.format(id_value)
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
def update_record_delivery_mill_minibar():
    
        today = date.today()
        username = getpass.getuser()
        now = datetime.now()
        date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
        query_parameters =json.loads(request.data)
        
        
        Market_Country=(query_parameters['Market_Country'])
        Market_Customer_Group=(query_parameters['Market_Customer_Group'])
        Market_Customer=(query_parameters['Market_Customer'])
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
           "Market - Customer Group"='{2}', "Market - Customer"='{3}', "Delivering Mill"='{4}',
           "Product Division"='{5}', 
           "Document Item Currency"='{6}',
           "Amount"='{7}',
           "Currency"=''{8}'',
           "updated_on"='{9}', sequence_id={10}
            WHERE "id"={11} '''.format(username,Market_Country,Market_Customer_Group,Market_Customer,Delivering_Mill,Product_Division,Document_Item_Currency,Amount,Currency,date_time,sequence_id,id_value)
            result1=db.insert(query2)
            print(query2)
            if result1=='failed' :raise ValueError
            
            return {"status":"success"},200
        except:
            return {"status":"failure"}
    
@smb_app1.route('/add_record_delivery_mill_minibar',methods=['POST'])
def add_record_delivery_mill_minibar():
    
    username = getpass.getuser()
    
    query_parameters =json.loads(request.data)
    
    
    Market_Country=(query_parameters['Market_Country'])
    Market_Customer_Group=(query_parameters['Market_Customer_Group'])
    Market_Customer=(query_parameters['Market_Customer'])
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    Product_Division =( query_parameters["Product_Division"])
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    try:
        input_tuple=(username,   Market_Country,Market_Customer_Group,Market_Customer,Delivering_Mill, Product_Division,Document_Item_Currency, Amount,Currency.strip("'"))
        query='''INSERT INTO "SMB"."SMB - Extra - Delivery Mill - MiniBar"(
             
             "Username",
             "Market - Country",
       "Market - Customer Group", "Market - Customer", "Delivering Mill",
       "Product Division", "Document Item Currency", "Amount", "Currency")
             VALUES{};'''.format(input_tuple)
             
        result=db.insert(query)  
        print(query)
        if result=='failed' :raise ValueError
        
        return {"status":"success"},200
        
    except:
        return {"status":"failure"},500


    
   
@smb_app1.route('/upload_delivery_mill_minibar', methods=['GET','POST'])
def upload_delivery_mill_minibar():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","Market - Country",
       "Market - Customer Group", "Market - Customer", "Delivering Mill",
       "Product Division", "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            df_main = pd.read_sql('''select "id","Market - Country",
       "Market - Customer Group", "Market - Customer", "Delivering Mill",
       "Product Division", "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Delivery Mill - MiniBar" where "active"='1' order by "id" ''', con=con)
            
            
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
               "Market_Customer_Group", "Market_Customer", "Delivering_Mill",
               "Product_Division", "Document_Item_Currency", "Amount", "Currency","date_time","id"]]
                
                query1='''INSERT INTO "SMB"."SMB - Extra - Delivery Mill - MiniBar_History"
                SELECT 
                "id",
                "Username",now(),
                "Market - Country",
               "Market - Customer Group", "Market - Customer", "Delivering Mill",
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
               "updated_on"='%s'
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
            df = pd.read_sql('''select * from "SMB"."SMB - Extra - Delivery Mill - MiniBar" where "active"='1' order by "id" ''', con=con)
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
        

    
   


