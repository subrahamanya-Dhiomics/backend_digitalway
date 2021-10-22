# -*- coding: utf-8 -*-
"""
Created on Wed Oct 20 10:07:00 2021

@author: Administrator
"""



from flask import Blueprint


import pandas as pd
import time
import json
from flask import Flask, request, render_template
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
            var = ''
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
               
 
smb_app2 = Blueprint('smb_app2', __name__)

CORS(smb_app2)

db=Database()

input_directory="C:/Users/Administrator/Documents/SMB_INPUT/"
con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')



# *****************************************************************************************************************************************
# freight_parity

@smb_app2.route('/data_freight_parity',methods=['GET','POST'])
def  freight_parity():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    query='''select max("date_time") from "SMB"."SMB - Extra - Freight Parity"'''
    
    date_time_raw=db.query(query)
    date_time= date_time_raw[0][0].strftime("%m/%d/%Y, %H:%M:%S")
    
    try:
        search_string=int(search_string)
    except:
        search_string=search_string   
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Freight Parity" where extract(month from "date_time")=extract(month from now()) order by "id" desc OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Freight Parity"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count,"date_time":date_time},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app2.route('/delete_record_freight_parity',methods=['POST','GET','DELETE'])
def delete_record_delivery_mill_minibar():  
    id_value=request.args.get('id')
    try:
        query='''delete  from "SMB"."SMB - Extra - Freight Parity" where "id"={}'''.format(id_value)
        db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app2.route('/get_record_freight_parity',methods=['GET','POST'])       
def get_record_freight_parity():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Freight Parity" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app2.route('/add_record_freight_parity',methods=['POST'])
def add_record_frieght_parity():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    Market_Country=(query_parameters['Market_Country'])
    Zip_Code=(query_parameters['Zip_Code_(Dest)'])
    
    Product_Division =( query_parameters["Product_Division"])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    
  
   
    id_value=db.query('select max("id") from "SMB"."SMB - Extra - Freight Parity"')
    id_value=(id_value[0][0]+1)
   
    input_tuple=(id_value, username, date_time,Delivering_Mill,Market_Country,Zip_Code, Product_Division,Document_Item_Currency, Amount, Currency.strip("'"))
    
    query='''INSERT INTO "SMB"."SMB - Extra - Freight Parity"(
         "id",
         "Username",
         "date_time",
         "Delivering_Mill",
         "Market_Country",
       "Zip_Code_(Dest)", 
       "Product Division",
       
      
         "Document Item Currency",
         "Amount",
         "Currency")
         VALUES{};'''.format(input_tuple)
    print(query)
         
   
    db.insert(query)
    return {"status":"success"},200

   
@smb_app2.route('/upload_freight_parity', methods=['GET','POST'])
def upload_freight_parity():
    
    f=request.files['filename']
    try:
            
        f.save(input_directory+f.filename)
        smb_df=pd.read_excel(input_directory+f.filename)
        
        df=smb_df[['id', 'Username', 'date_time', 'Delivering Mill', 'Market - Country',
       'Zip Code (Dest)', 'Product Division', 'Document Item Currency',
       'Amount', 'Currency']]  
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table},200
    except:
        return {"statuscode":500,"message":"incorrect"},500


@smb_app2.route('/validate_freight_parity', methods=['GET','POST'])
def  validate_freight_parity():
    
        
        json_data=json.loads(request.data)
        
        try:
            df=pd.DataFrame(json_data["billet"])  
            
            username = getpass.getuser()
            now = datetime.now()
            date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
            
            id_value=db.query('select max("id") from "SMB"."SMB - Extra - Freight Parity"')
            id_value=(id_value[0][0]+1)
            
            df.columns = df.columns.str.replace('_', ' ')
            df.rename(columns={"Market Country":"Market - Country"},inplace=True)
            
            
            
            df.insert(0, 'id', range(id_value, id_value + len(df)))
            df.insert(1,'Username',username)
            df.insert(2,'date_time',date_time)
            
            df['id']=df['id'].astype(int)
            df['date_time']=pd.to_datetime(df['date_time'])
            
            df.to_sql("SMB - Extra - Freight Parity",con=engine, schema='SMB',if_exists='append', index=False)
            
            return {"status":"success"},200
        except:
            return {"status":"failure"},500
        
         
@smb_app2.route('/download_freight_parity',methods=['GET'])
def download_freight_parity():
   
        now = datetime.now()
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Freight Parity" where extract(month from "date_time")=extract(month from now()) order by "id" desc''', con=con)
        df.drop(['Username','date_time','id'],axis=1,inplace=True)
        df.to_excel('C:/Users/Administrator/Downloads/'+now.strftime("%d-%m-%Y-%H-%M-%S") +'bse_price.xlsx',index=False)
        return {"status":"success"},200


# *****************************************************************************************************************************************
# freight_parity_minibar


@smb_app2.route('/data_freight_parity_minibar',methods=['GET','POST'])
def  freight_parity_minibar():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    query='''select max("date_time") from "SMB"."SMB - Extra - Freight Parity - MiniBar"'''
    
    date_time_raw=db.query(query)
    date_time= date_time_raw[0][0].strftime("%m/%d/%Y, %H:%M:%S")
    
    try:
        search_string=int(search_string)
    except:
        search_string=search_string   
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Freight Parity - MiniBar" where extract(month from "date_time")=extract(month from now()) order by "id" desc OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Freight Parity - MiniBar"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
                
            
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count,"date_time":date_time},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app2.route('/delete_record_freight_parity_minibar',methods=['POST','GET','DELETE'])
def delete_record_freight_parity_minibar():  
    id_value=request.args.get('id')
    try:
        query='''delete  from "SMB"."SMB - Extra - Freight Parity - MiniBar"" where "id"={}'''.format(id_value)
        db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app2.route('/get_record_freight_parity_minibar',methods=['GET','POST'])       
def get_record_freight_parity_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Freight Parity - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
    
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app2.route('/add_record_freight_parity_minibar',methods=['POST'])
def add_record_frieght_parity_minibar():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    Market_Country=(query_parameters['Market_Country'])
    Market_Customer_Group=(query_parameters['Market_Customer_Group'])
    Market_Customer=(query_parameters['Market_Customer'])
   
    Zip_Code=(query_parameters['Zip_Code_(Dest)'])
    
    Product_Division =( query_parameters["Product_Division"])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    
  
   
    id_value=db.query('select max("id") from "SMB"."SMB - Extra - Freight Parity - MiniBar"')
    id_value=(id_value[0][0]+1)
   
    input_tuple=(id_value, username, date_time,Delivering_Mill,Market_Country,Market_Customer_Group,Market_Customer,Zip_Code, Product_Division,Document_Item_Currency, Amount, Currency.strip("'"))
    
    query='''INSERT INTO "SMB"."SMB - Extra - Freight Parity - MiniBar"(
         "id",
         "Username",
         "date_time",
         "Delivering_Mill",
         "Market_Country",
         "Market - Customer Group",
         "Market - Customer",
       "Zip_Code_(Dest)", 
       "Product Division",
       
         "Document Item Currency",
         "Amount",
         "Currency")
         VALUES{};'''.format(input_tuple)
    print(query)
         
   
    db.insert(query)
    return {"status":"success"},200

   
@smb_app2.route('/upload_freight_parity_minibar', methods=['GET','POST'])
def upload_freight_parity_minibar():
    
    f=request.files['filename']
    try:
            
        f.save(input_directory+f.filename)
        smb_df=pd.read_excel(input_directory+f.filename)
        
        df=smb_df[['id', 'Username', 'date_time', 'Delivering Mill', 'Market - Country',
       'Market - Customer Group', 'Market - Customer', 'Zip Code (Dest)',
       'Product Division', 'Document Item Currency', 'Amount', 'Currency']]  
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
    
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table},200
    except:
        return {"statuscode":500,"message":"incorrect"},500


@smb_app2.route('/validate_freight_parity_minibar', methods=['GET','POST'])
def  validate_freight_parity_minibar():
    
        
        json_data=json.loads(request.data)
        
        try:
            df=pd.DataFrame(json_data["billet"])  
            
            username = getpass.getuser()
            now = datetime.now()
            date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
            
            id_value=db.query('select max("id") from "SMB"."SMB - Extra - Freight Parity - MiniBar"')
            id_value=(id_value[0][0]+1)
            
            df.columns = df.columns.str.replace('_', ' ')
            df.rename(columns={"Market Country":"Market - Country","Market Customer Group":"Market - Customer Group","Market Customer":"Market - Customer"},inplace=True)
            
            
            
            df.insert(0, 'id', range(id_value, id_value + len(df)))
            df.insert(1,'Username',username)
            df.insert(2,'date_time',date_time)
            
            df['id']=df['id'].astype(int)
            df['date_time']=pd.to_datetime(df['date_time'])
            
            df.to_sql("SMB - Extra - Freight Parity - MiniBar",con=engine, schema='SMB',if_exists='append', index=False)
            
            return {"status":"success"},200
        except:
            return {"status":"failure"},500
        
         
@smb_app2.route('/download_freight_parity_minibar',methods=['GET'])
def download_freight_parity_minibar():
   
        now = datetime.now()
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Freight Parity - MiniBar" where extract(month from "date_time")=extract(month from now()) order by "id" desc''', con=con)
        df.drop(['Username','date_time','id'],axis=1,inplace=True)
        df.to_excel('C:/Users/Administrator/Downloads/'+now.strftime("%d-%m-%Y-%H-%M-%S") +'bse_price.xlsx',index=False)
        return {"status":"success"},200



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
    
    query='''select max("date_time") from "SMB"."SMB - Extra - Grade"'''
    
    date_time_raw=db.query(query)
    date_time= date_time_raw[0][0].strftime("%m/%d/%Y, %H:%M:%S")
    
    try:
        search_string=int(search_string)
    except:
        search_string=search_string   
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Grade" where extract(month from "date_time")=extract(month from now()) order by "id" desc OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Grade"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
                
            
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count,"date_time":date_time},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app2.route('/delete_record_extra_grade',methods=['POST','GET','DELETE'])
def delete_record_extra_grade():  
    id_value=request.args.get('id')
    try:
        query='''delete  from "SMB"."SMB - Extra - Grade"" where "id"={}'''.format(id_value)
        db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app2.route('/get_record_extra_grade',methods=['GET','POST'])       
def get_record_extra_grade():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Grade" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
    
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app2.route('/add_record_extra_grade',methods=['POST'])
def add_record_extra_grade():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    
    
    BusinessCode=(query_parameters["BusinessCode"])
    Grade_Category=(query_parameters["Grade_Category"])
    Country_Group=(query_parameters['Country_Group'])
    Market_Country=(query_parameters['Market_Country'])
    
    
    Product_Division =( query_parameters["Product_Division"])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    try:
        
  
   
        id_value=db.query('select max("id") from "SMB"."SMB - Extra - Grade"')
        id_value=(id_value[0][0]+1)
       
        input_tuple=(id_value, username, date_time,BusinessCode,Grade_Category,Country_Group,Market_Country, Product_Division,Document_Item_Currency, Amount, Currency.strip("'"))
        
        query='''INSERT INTO "SMB"."SMB - Extra - Grade"(
             "id",
             "Username",
             "date_time",
             "BusinessCode",
             "Grade Category",
           "Country Group",
           "Market - Country",
           "Product Division",
             "Document Item Currency",
             "Amount",
             "Currency")
             VALUES{};'''.format(input_tuple)
         
       
        db.insert(query)
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
   
@smb_app2.route('/upload_extra_grade', methods=['GET','POST'])
def upload_extra_grade():
    
    f=request.files['filename']
    try:
            
        f.save(input_directory+f.filename)
        smb_df=pd.read_excel(input_directory+f.filename)
        
        df=smb_df[['id', 'Username', 'date_time', 'BusinessCode', 'Grade Category',
       'Country Group', 'Market - Country', 'Product Division',
       'Document Item Currency', 'Amount', 'Currency']]  
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
    
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table},200
    except:
        return {"statuscode":500,"message":"incorrect"},500


@smb_app2.route('/validate_extra_grade', methods=['GET','POST'])
def  validate_extra_grade():
    
        
        json_data=json.loads(request.data)
        
        try:
            df=pd.DataFrame(json_data["billet"])  
            
            username = getpass.getuser()
            now = datetime.now()
            date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
            
            id_value=db.query('select max("id") from "SMB"."SMB - Extra - Grade"')
            id_value=(id_value[0][0]+1)
            
            df.columns = df.columns.str.replace('_', ' ')
            df.rename(columns={"Market Country":"Market - Country"},inplace=True)
            
            
            
            df.insert(0, 'id', range(id_value, id_value + len(df)))
            df.insert(1,'Username',username)
            df.insert(2,'date_time',date_time)
            
            df['id']=df['id'].astype(int)
            df['date_time']=pd.to_datetime(df['date_time'])
            
            df.to_sql("SMB - Extra - Grade",con=engine, schema='SMB',if_exists='append', index=False)
            
            return {"status":"success"},200
        except:
            return {"status":"failure"},500
        
         
@smb_app2.route('/download_freight_extra_grade',methods=['GET'])
def download_extra_grade():
   
        now = datetime.now()
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Grade" where extract(month from "date_time")=extract(month from now()) order by "id" desc''', con=con)
        df.drop(['Username','date_time','id'],axis=1,inplace=True)
        df.to_excel('C:/Users/Administrator/Downloads/'+now.strftime("%d-%m-%Y-%H-%M-%S") +'bse_price.xlsx',index=False)
        return {"status":"success"},200

# ***********************************************************************************************************************************************************************

# ***********************************************************************************************************************************************************************
# "SMB"."SMB - Extra - Grade - MiniBar"



@smb_app2.route('/data_extra_grade_minibar',methods=['GET','POST'])
def  extra_grade_data_minibar():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    query='''select max("date_time") from "SMB"."SMB - Extra - Grade - MiniBar"'''
    
    date_time_raw=db.query(query)
    date_time= date_time_raw[0][0].strftime("%m/%d/%Y, %H:%M:%S")
    
    try:
        search_string=int(search_string)
    except:
        search_string=search_string   
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Grade - MiniBar" where extract(month from "date_time")=extract(month from now()) order by "id" desc OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Grade - MiniBar"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)  
                
            
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count,"date_time":date_time},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app2.route('/delete_record_extra_grade_minibar',methods=['POST','GET','DELETE'])
def delete_record_extra_grade_minibar():  
    id_value=request.args.get('id')
    try:
        query='''delete  from "SMB"."SMB - Extra - Grade - MiniBar" where "id"={}'''.format(id_value)
        db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app2.route('/get_record_extra_grade_minibar',methods=['GET','POST'])       
def get_record_extra_grade_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Grade - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)  
         
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app2.route('/add_record_extra_grade_minibar',methods=['POST'])
def add_record_extra_grade_minibar():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    
    
    BusinessCode=(query_parameters["BusinessCode"])
    Customer_Group=(query_parameters["Customer_Group"])
    
    Market_Customer=(query_parameters['Market_Customer'])
    
    Market_Country=(query_parameters['Market_Country'])
    
    Grade_Category=(query_parameters['Grade_Category'])
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    try:
        
  
   
        id_value=db.query('select max("id") from "SMB"."SMB - Extra - Grade - MiniBar"')
        id_value=(id_value[0][0]+1)
       
        input_tuple=(id_value, username, date_time,BusinessCode,Customer_Group,Market_Customer,Market_Country,Grade_Category ,Document_Item_Currency, Amount, Currency.strip("'"))
        
        query='''INSERT INTO "SMB"."SMB - Extra - Grade - MiniBar"(
             "id",
             "Username",
             "date_time",
             "BusinessCode",
             "Customer Group",
           "Market - Customer",
           "Market - Country",
           "Grade Category",
             "Document Item Currency",
             "Amount",
             "Currency")
             VALUES{};'''.format(input_tuple)
         
       
        db.insert(query)
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
   
@smb_app2.route('/upload_extra_grade_minibar', methods=['GET','POST'])
def upload_extra_grade_minibar():
    
    f=request.files['filename']
    try:
            
        f.save(input_directory+f.filename)
        smb_df=pd.read_excel(input_directory+f.filename)
        
        df=smb_df[['id', 'Username', 'date_time', 'BusinessCode', 'Customer Group',
       'Market - Customer', 'Market - Country', 'Grade Category',
       'Document Item Currency', 'Amount', 'Currency']]  
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)  
    
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table},200
    except:
        return {"statuscode":500,"message":"incorrect"},500


@smb_app2.route('/validate_extra_grade_minibar', methods=['GET','POST'])
def  validate_extra_grade_minibar():
    
        
        json_data=json.loads(request.data)
        
        try:
            df=pd.DataFrame(json_data["billet"])  
            
            username = getpass.getuser()
            now = datetime.now()
            date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
            
            id_value=db.query('select max("id") from "SMB"."SMB - Extra - Grade - MiniBar"')
            id_value=(id_value[0][0]+1)
            
            df.columns = df.columns.str.replace('_', ' ')
            df.rename(columns={"Market Country":"Market - Country","Market Customer":"Market - Customer"},inplace=True)
            
            
            
            df.insert(0, 'id', range(id_value, id_value + len(df)))
            df.insert(1,'Username',username)
            df.insert(2,'date_time',date_time)
            
            df['id']=df['id'].astype(int)
            df['date_time']=pd.to_datetime(df['date_time'])
            
            df.to_sql("SMB - Extra - Grade - MiniBar",con=engine, schema='SMB',if_exists='append', index=False)
            
            return {"status":"success"},200
        except:
            return {"status":"failure"},500
        
         
@smb_app2.route('/download_extra_grade_minibar',methods=['GET'])
def download_extra_grade_minibar():
   
        now = datetime.now()
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Grade - MiniBar" where extract(month from "date_time")=extract(month from now()) order by "id" desc''', con=con)
        df.drop(['Username','date_time','id'],axis=1,inplace=True)
        df.to_excel('C:/Users/Administrator/Downloads/'+now.strftime("%d-%m-%Y-%H-%M-%S") +'bse_price.xlsx',index=False)
        return {"status":"success"},200



# ***********************************************************************************************************************************************************************
# "SMB"."SMB - Extra - Profile"



@smb_app2.route('/data_extra_profile',methods=['GET','POST'])
def  extra_profile():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    query='''select max("date_time") from "SMB"."SMB - Extra - Profile"'''
    
    date_time_raw=db.query(query)
    date_time= date_time_raw[0][0].strftime("%m/%d/%Y, %H:%M:%S")
    
    try:
        search_string=int(search_string)
    except:
        search_string=search_string   
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Profile" where extract(month from "date_time")=extract(month from now()) order by "id" desc OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Profile"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
                
            
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count,"date_time":date_time},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app2.route('/delete_record_extra_profile',methods=['POST','GET','DELETE'])
def delete_record_extra_profile():  
    id_value=request.args.get('id')
    try:
        query='''delete  from "SMB"."SMB - Extra - Profile" where "id"={}'''.format(id_value)
        db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app2.route('/get_record_extra_profile',methods=['GET','POST'])       
def get_record_extra_profile():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Profile" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
         
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app2.route('/add_record_extra_profile',methods=['POST'])
def add_record_extra_profile():
    
    today = date.today()
    username = getpass.getuser()
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
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    try:
        
  
   
        id_value=db.query('select max("id") from "SMB"."SMB - Extra - Profile"')
        id_value=(id_value[0][0]+1)
       
        input_tuple=(id_value, username, date_time,BusinessCode,Market_Country,Product_Division,Product_Level_04,Product_Level_05,Product_Level_02,Delivering_Mill,Document_Item_Currency, Amount, Currency.strip("'"))
        
        query='''INSERT INTO "SMB"."SMB - Extra - Profile"(
             "id",
             "Username",
             "date_time",
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
             VALUES{};'''.format(input_tuple)
         
       
        db.insert(query)
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
   
@smb_app2.route('/upload_extra_profile', methods=['GET','POST'])
def upload_extra_profile():
    
    f=request.files['filename']
    try:
            
        f.save(input_directory+f.filename)
        smb_df=pd.read_excel(input_directory+f.filename)
        
        df=smb_df[['id', 'Username', 'date_time', 'BusinessCode', 'Market - Country',
       'Product Division', 'Product Level 04', 'Product Level 05',
       'Product Level 02', 'Delivering Mill', 'Document Item Currency',
       'Amount', 'Currency']]  
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
    
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table},200
    except:
        return {"statuscode":500,"message":"incorrect"},500


@smb_app2.route('/validate_extra_profile', methods=['GET','POST'])
def  validate_extra_profile():
    
        
        json_data=json.loads(request.data)
        
        try:
            df=pd.DataFrame(json_data["billet"])  
            
            username = getpass.getuser()
            now = datetime.now()
            date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
            
            id_value=db.query('select max("id") from "SMB"."SMB - Extra - Profile"')
            id_value=(id_value[0][0]+1)
            
            df.columns = df.columns.str.replace('_', ' ')
            df.rename(columns={"Market Country":"Market - Country"},inplace=True)
            
            
            
            df.insert(0, 'id', range(id_value, id_value + len(df)))
            df.insert(1,'Username',username)
            df.insert(2,'date_time',date_time)
            
            df['id']=df['id'].astype(int)
            df['date_time']=pd.to_datetime(df['date_time'])
            
            df.to_sql("SMB - Extra - Profile",con=engine, schema='SMB',if_exists='append', index=False)
            
            return {"status":"success"},200
        except:
            return {"status":"failure"},500
        
         
@smb_app2.route('/download_extra_profile',methods=['GET'])
def download_extra_profile():
   
        now = datetime.now()
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Profile" where extract(month from "date_time")=extract(month from now()) order by "id" desc''', con=con)
        df.drop(['Username','date_time','id'],axis=1,inplace=True)
        df.to_excel('C:/Users/Administrator/Downloads/'+now.strftime("%d-%m-%Y-%H-%M-%S") +'bse_price.xlsx',index=False)
        return {"status":"success"},200



# ***********************************************************************************************************************************************************************
# "SMB"."SMB - Extra - Profile - MiniBar"



@smb_app2.route('/data_extra_profile_minibar',methods=['GET','POST'])
def  extra_profile_minibar():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    query='''select max("date_time") from "SMB"."SMB - Extra - Profile - MiniBar"'''
    
    date_time_raw=db.query(query)
    date_time= date_time_raw[0][0].strftime("%m/%d/%Y, %H:%M:%S")
    
    try:
        search_string=int(search_string)
    except:
        search_string=search_string   
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Profile - MiniBar" where extract(month from "date_time")=extract(month from now()) order by "id" desc OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Profile - MiniBar"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
           
            
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count,"date_time":date_time},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app2.route('/delete_record_extra_profile_minibar',methods=['POST','GET','DELETE'])
def delete_record_extra_profile_minibar():  
    id_value=request.args.get('id')
    try:
        query='''delete  from "SMB"."SMB - Extra - Profile - MiniBar" where "id"={}'''.format(id_value)
        db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app2.route('/get_record_extra_profile_minibar',methods=['GET','POST'])       
def get_record_extra_profile_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Profile - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
         
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app2.route('/add_record_extra_profile_minibar',methods=['POST'])
def add_record_extra_profile_minibar():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    
    
    BusinessCode=(query_parameters["BusinessCode"])
    Customer_Group=(query_parameters['Customer_Group'])
    Market_Customer=(query_parameters['Market_Customer'])
    Market_Country=(query_parameters['Market_Country'])
    Product_Division=(query_parameters['Product_Division'])
    Product_Level_04=(query_parameters['Product_Level_04'])
    Product_Level_05=(query_parameters['Product_Level_05'])
    Product_Level_02=(query_parameters['Product_Level_02'])
    Delivering_Mill=(query_parameters['Delivering_Mill'])
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    try:
        
  
   
        id_value=db.query('select max("id") from "SMB"."SMB - Extra - Profile - MiniBar"')
        id_value=(id_value[0][0]+1)
       
        input_tuple=(id_value, username, date_time,BusinessCode,Market_Country,Product_Division,Product_Level_04,Product_Level_05,Product_Level_02,Delivering_Mill,Document_Item_Currency, Amount, Currency.strip("'"))
        
        query='''INSERT INTO "SMB"."SMB - Extra - Profile - MiniBar"(
             "id",
             "Username",
             "date_time",
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
             VALUES{};'''.format(input_tuple)
         
       
        db.insert(query)
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
   
@smb_app2.route('/upload_extra_profile_minibar', methods=['GET','POST'])
def upload_extra_profile_minibar():
    
    f=request.files['filename']
    try:
            
        f.save(input_directory+f.filename)
        smb_df=pd.read_excel(input_directory+f.filename)
        
        df=smb_df[['id', 'Username', 'date_time', 'BusinessCode', 'Customer Group',
       'Market - Customer', 'Market - Country', 'Product Level 04',
       'Product Level 05', 'Product Level 02', 'Delivering Mill',
       'Document Item Currency', 'Amount', 'Currency']]  
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
         
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table},200
    except:
        return {"statuscode":500,"message":"incorrect"},500


@smb_app2.route('/validate_extra_profile_minibar', methods=['GET','POST'])
def  validate_extra_profile_minibar():
    
        
        json_data=json.loads(request.data)
        
        try:
            df=pd.DataFrame(json_data["billet"])  
            
            username = getpass.getuser()
            now = datetime.now()
            date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
            
            id_value=db.query('select max("id") from "SMB"."SMB - Extra - Profile - MiniBar"')
            id_value=(id_value[0][0]+1)
            
            df.columns = df.columns.str.replace('_', ' ')
            df.rename(columns={"Market Country":"Market - Country","Market Customer":"Market - Customer"},inplace=True)
            
            
            
            df.insert(0, 'id', range(id_value, id_value + len(df)))
            df.insert(1,'Username',username)
            df.insert(2,'date_time',date_time)
            
            df['id']=df['id'].astype(int)
            df['date_time']=pd.to_datetime(df['date_time'])
            
            df.to_sql("SMB - Extra - Profile - MiniBar",con=engine, schema='SMB',if_exists='append', index=False)
            
            return {"status":"success"},200
        except:
            return {"status":"failure"},500
        
         
@smb_app2.route('/download_extra_profile_minibar',methods=['GET'])
def download_extra_profile_minibar():
   
        now = datetime.now()
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Profile - MiniBar" where extract(month from "date_time")=extract(month from now()) order by "id" desc''', con=con)
        df.drop(['Username','date_time','id'],axis=1,inplace=True)
        df.to_excel('C:/Users/Administrator/Downloads/'+now.strftime("%d-%m-%Y-%H-%M-%S") +'bse_price.xlsx',index=False)
        return {"status":"success"},200



