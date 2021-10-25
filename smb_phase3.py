# -*- coding: utf-8 -*-
"""
Created on Sun Oct 24 07:12:53 2021

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
               
 
smb_app3 = Blueprint('smb_app3', __name__)

CORS(smb_app3)

db=Database()

input_directory="C:/Users/Administrator/Documents/SMB_INPUT/"
con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')



# *****************************************************************************************************************************************
# transport mode

@smb_app3.route('/data_transport',methods=['GET','POST'])
def  data_transport():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    query='''select max("date_time") from "SMB"."SMB - Extra - Transport Mode"'''
    
    date_time_raw=db.query(query)
    date_time= date_time_raw[0][0].strftime("%m/%d/%Y, %H:%M:%S")
    
    try:
        search_string=int(search_string)
    except:
        search_string=search_string   
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Transport Mode" where extract(month from "date_time")=extract(month from now()) order by "id" desc OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Transport Mode"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count,"date_time":date_time},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app3.route('/delete_record_transport',methods=['POST','GET','DELETE'])
def delete_record_transport():  
    id_value=request.args.get('id')
    try:
        query='''delete  from "SMB"."SMB - Extra - Transport Mode" where "id"={}'''.format(id_value)
        db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app3.route('/get_record_transport',methods=['GET','POST'])       
def get_record_transport():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Transport Mode" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app3.route('/add_record_transport',methods=['POST'])
def add_record_transport():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    Product_Division =( query_parameters["Product_Division"])
    Market_Country=(query_parameters['Market_Country'])
    
    Transport_Mode=(query_parameters["Transport_Mode"])
    
   
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    
  
   
    id_value=db.query('select max("id") from "SMB"."SMB - Extra - Transport Mode"')
    id_value=(id_value[0][0]+1)
   
    input_tuple=(id_value, username, date_time,Product_Division,Market_Country,Transport_Mode,Document_Item_Currency, Amount, Currency.strip("'"))
    
    query='''INSERT INTO "SMB"."SMB - Extra - Transport Mode"(
         "id",
         "Username",
         "date_time",
         "Product Division", 
         "Market - Country",
       "Transport Mode",
       
      
         "Document Item Currency",
         "Amount",
         "Currency")
         VALUES{};'''.format(input_tuple)
    print(query)
         
   
    db.insert(query)
    return {"status":"success"},200

   
@smb_app3.route('/upload_transport', methods=['GET','POST'])
def upload_transport():
    
    f=request.files['filename']
    try:
            
        f.save(input_directory+f.filename)
        smb_df=pd.read_excel(input_directory+f.filename)
        
        df=smb_df[['id', 'Username', 'date_time', 'Product Division', 'Market - Country',
       'Transport Mode', 'Document Item Currency', 'Amount', 'Currency']]  
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table},200
    except:
        return {"statuscode":500,"message":"incorrect"},500


@smb_app3.route('/validate_transport', methods=['GET','POST'])
def  validate_transport():
    
        
        json_data=json.loads(request.data)
        
        try:
            df=pd.DataFrame(json_data["billet"])  
            
            username = getpass.getuser()
            now = datetime.now()
            date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
            
            id_value=db.query('select max("id") from "SMB"."SMB - Extra - Transport Mode"')
            id_value=(id_value[0][0]+1)
            
            df.columns = df.columns.str.replace('_', ' ')
            df.rename(columns={"Market Country":"Market - Country"},inplace=True)
            
            
            
            df.insert(0, 'id', range(id_value, id_value + len(df)))
            df.insert(1,'Username',username)
            df.insert(2,'date_time',date_time)
            
            df['id']=df['id'].astype(int)
            df['date_time']=pd.to_datetime(df['date_time'])
            
            df.to_sql("SMB - Extra - Transport Mode",con=engine, schema='SMB',if_exists='append', index=False)
            
            return {"status":"success"},200
        except:
            return {"status":"failure"},500
        
         
@smb_app3.route('/download_transport',methods=['GET'])
def download_transport():
   
        now = datetime.now()
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Transport Mode" where extract(month from "date_time")=extract(month from now()) order by "id" desc''', con=con)
        df.drop(['Username','date_time','id'],axis=1,inplace=True)
        df.to_excel('C:/Users/Administrator/Downloads/'+now.strftime("%d-%m-%Y-%H-%M-%S") +'bse_price.xlsx',index=False)
        return {"status":"success"},200



# *****************************************************************************************************************************************
# transport mode Minibar

@smb_app3.route('/data_transport_minibar',methods=['GET','POST'])
def  data_transport_minibar():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    query='''select max("date_time") from "SMB"."SMB - Extra - Transport Mode - MiniBar"'''
    
    date_time_raw=db.query(query)
    date_time= date_time_raw[0][0].strftime("%m/%d/%Y, %H:%M:%S")
    
    try:
        search_string=int(search_string)
    except:
        search_string=search_string   
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Transport Mode - MiniBar" where extract(month from "date_time")=extract(month from now()) order by "id" desc OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Transport Mode - MiniBar"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
         
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count,"date_time":date_time},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app3.route('/delete_record_transport_minibar',methods=['POST','GET','DELETE'])
def delete_record_transport_minibar():  
    id_value=request.args.get('id')
    try:
        query='''delete  from "SMB"."SMB - Extra - Transport Mode - MiniBar" where "id"={}'''.format(id_value)
        db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app3.route('/get_record_transport_minibar',methods=['GET','POST'])       
def get_record_transport_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Transport Mode - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app3.route('/add_record_transport_minibar',methods=['POST'])
def add_record_transport_minibar():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    Product_Division =( query_parameters["Product_Division"])
    Market_Country=(query_parameters['Market_Country'])
    Market_Customer_Group=(query_parameters['Market_Customer_Group'])
    Market_Customer=(query_parameters['Market_Customer'])
    
    Transport_Mode=(query_parameters["Transport_Mode"])
    
   
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    
  
   
    id_value=db.query('select max("id") from "SMB"."SMB - Extra - Transport Mode - MiniBar"')
    id_value=(id_value[0][0]+1)
   
    input_tuple=(id_value, username, date_time,Product_Division,Market_Country,Market_Customer_Group,Market_Customer_Group,Transport_Mode,Document_Item_Currency, Amount, Currency.strip("'"))
    
    query='''INSERT INTO "SMB"."SMB - Extra - Transport Mode - MiniBar"(
         "id",
         "Username",
         "date_time",
         "Product Division", 
         "Market - Country",
         "Market - Customer Group",
         "Market - Customer",
       "Transport Mode",
       
      
         "Document Item Currency",
         "Amount",
         "Currency")
         VALUES{};'''.format(input_tuple)
    print(query)
         
   
    db.insert(query)
    return {"status":"success"},200

   
@smb_app3.route('/upload_transport_minibar', methods=['GET','POST'])
def upload_transport_minibar():
    
    f=request.files['filename']
    try:
            
        f.save(input_directory+f.filename)
        smb_df=pd.read_excel(input_directory+f.filename)
        
        df=smb_df[['id', 'Username', 'date_time', 'Product Division', 'Market - Country',
       'Market - Customer Group', 'Market - Customer', 'Transport Mode',
       'Document Item Currency', 'Amount', 'Currency']]  
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table},200
    except:
        return {"statuscode":500,"message":"incorrect"},500


@smb_app3.route('/validate_transport_minibar', methods=['GET','POST'])
def  validate_transport_minibar():
    
        
        json_data=json.loads(request.data)
        
        try:
            df=pd.DataFrame(json_data["billet"])  
            
            username = getpass.getuser()
            now = datetime.now()
            date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
            
            id_value=db.query('select max("id") from "SMB"."SMB - Extra - Transport Mode - MiniBar"')
            id_value=(id_value[0][0]+1)
            
            df.columns = df.columns.str.replace('_', ' ')
            df.rename(columns={"Market Country":"Market - Country","Market Customer Group":"Market - Customer Group","Market Customer":"Market - Customer"},inplace=True)
            
            
            
            df.insert(0, 'id', range(id_value, id_value + len(df)))
            df.insert(1,'Username',username)
            df.insert(2,'date_time',date_time)
            
            df['id']=df['id'].astype(int)
            df['date_time']=pd.to_datetime(df['date_time'])
            
            df.to_sql("SMB - Extra - Transport Mode - MiniBar",con=engine, schema='SMB',if_exists='append', index=False)
            
            return {"status":"success"},200
        except:
            return {"status":"failure"},500
        
         
@smb_app3.route('/download_transport_minibar',methods=['GET'])
def download_transport_minibar():
   
        now = datetime.now()
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Transport Mode - MiniBar" where extract(month from "date_time")=extract(month from now()) order by "id" desc''', con=con)
        df.drop(['Username','date_time','id'],axis=1,inplace=True)
        df.to_excel('C:/Users/Administrator/Downloads/'+now.strftime("%d-%m-%Y-%H-%M-%S") +'bse_price.xlsx',index=False)
        return {"status":"success"},200



# *****************************************************************************************************************************************
# "SMB"."SMB - Extra - Length Production"


@smb_app3.route('/data_length_production',methods=['GET','POST'])
def  data_length_production():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    query='''select max("date_time") from "SMB"."SMB - Extra - Length Production"'''
    
    date_time_raw=db.query(query)
    date_time= date_time_raw[0][0].strftime("%m/%d/%Y, %H:%M:%S")
    
    try:
        search_string=int(search_string)
    except:
        search_string=search_string   
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Length Production" where extract(month from "date_time")=extract(month from now()) order by "id" desc OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Length Production"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count,"date_time":date_time},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app3.route('/delete_record_length_production',methods=['POST','GET','DELETE'])
def delete_record_length_production():  
    id_value=request.args.get('id')
    try:
        query='''delete  from "SMB"."SMB - Extra - Length Production" where "id"={}'''.format(id_value)
        db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app3.route('/get_record_length_production',methods=['GET','POST'])       
def get_record_length_production():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Length Production" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app3.route('/add_record_length_production',methods=['POST'])
def add_record_length_production():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Country_Group=(query_parameters['Country_Group'])
    Market_Country=(query_parameters['Market_Country'])
    
    Delivering_Mill=(query_parameters['Delivering_Mill'])
    Length=(query_parameters['Length'])
    Length_From=(query_parameters['Length_From'])
    Length_To=(query_parameters['Length_From'])
    
   
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    
  
   
    id_value=db.query('select max("id") from "SMB"."SMB - Extra - Length Production"')
    id_value=(id_value[0][0]+1)
   
    input_tuple=(id_value, username, date_time,BusinessCode,Country_Group,Market_Country,Delivering_Mill,Length,Length_From,Length_To,Document_Item_Currency, Amount, Currency.strip("'"))
    
    query='''INSERT INTO "SMB"."SMB - Extra - Length Production"(
         "id",
         "Username",
         "date_time",
         "BusinessCode",
         "Country Group",
       "Market - Country", 
       "Delivering Mill", 
       "Length",
       "Length From",
       "Length To",
         "Document Item Currency",
         "Amount",
         "Currency")
         VALUES{};'''.format(input_tuple)
    print(query)
         
   
    db.insert(query)
    return {"status":"success"},200

   
@smb_app3.route('/upload_length_production', methods=['GET','POST'])
def upload_length_production():
    
    f=request.files['filename']
    try:
            
        f.save(input_directory+f.filename)
        smb_df=pd.read_excel(input_directory+f.filename)
        
        df=smb_df[['id', 'Username', 'date_time', 'BusinessCode', 'Country Group',
       'Market - Country', 'Delivering Mill', 'Length', 'Length From',
       'Length To', 'Document Item Currency', 'Amount', 'Currency']]  
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table},200
    except:
        return {"statuscode":500,"message":"incorrect"},500


@smb_app3.route('/validate_length_production', methods=['GET','POST'])
def  validate_length_production():
    
        
        json_data=json.loads(request.data)
        
        try:
            df=pd.DataFrame(json_data["billet"])  
            
            username = getpass.getuser()
            now = datetime.now()
            date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
            
            id_value=db.query('select max("id") from "SMB"."SMB - Extra - Length Production"')
            id_value=(id_value[0][0]+1)
            
            df.columns = df.columns.str.replace('_', ' ')
            df.rename(columns={"Market Country":"Market - Country"},inplace=True)
            
            
            
            df.insert(0, 'id', range(id_value, id_value + len(df)))
            df.insert(1,'Username',username)
            df.insert(2,'date_time',date_time)
            
            df['id']=df['id'].astype(int)
            df['date_time']=pd.to_datetime(df['date_time'])
            
            df.to_sql("SMB - Extra - Length Production",con=engine, schema='SMB',if_exists='append', index=False)
            
            return {"status":"success"},200
        except:
            return {"status":"failure"},500
        
         
@smb_app3.route('/download_length_production',methods=['GET'])
def download_length_production():
   
        now = datetime.now()
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Length Production" where extract(month from "date_time")=extract(month from now()) order by "id" desc''', con=con)
        df.drop(['Username','date_time','id'],axis=1,inplace=True)
        df.to_excel('C:/Users/Administrator/Downloads/'+now.strftime("%d-%m-%Y-%H-%M-%S") +'bse_price.xlsx',index=False)
        return {"status":"success"},200




# *****************************************************************************************************************************************
# "SMB"."SMB - Extra - Length Production minibar"


@smb_app3.route('/data_length_production_minibar',methods=['GET','POST'])
def  data_length_production_minibar():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    query='''select max("date_time") from "SMB"."SMB - Extra - Length Production - MiniBar"'''
    
    date_time_raw=db.query(query)
    date_time= date_time_raw[0][0].strftime("%m/%d/%Y, %H:%M:%S")
    
    try:
        search_string=int(search_string)
    except:
        search_string=search_string   
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Length Production - MiniBar" where extract(month from "date_time")=extract(month from now()) order by "id" desc OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Length Production - MiniBar"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count,"date_time":date_time},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app3.route('/delete_record_length_production_minibar',methods=['POST','GET','DELETE'])
def delete_record_length_production_minibar():  
    id_value=request.args.get('id')
    try:
        query='''delete  from "SMB"."SMB - Extra - Length Production - MiniBar" where "id"={}'''.format(id_value)
        db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app3.route('/get_record_length_production_minibar',methods=['GET','POST'])       
def get_record_length_production_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Length Production - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
        
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app3.route('/add_record_length_production_minibar',methods=['POST'])
def add_record_length_production_minibar():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Customer_Group=(query_parameters['Customer_Group'])
    
    Market_Customer=(query_parameters['Market_Customer'])
    Market_Country=(query_parameters['Market_Country'])
    
    Delivering_Mill=(query_parameters['Delivering_Mill'])
    Length=(query_parameters['Length'])
    Length_From=(query_parameters['Length_From'])
    Length_To=(query_parameters['Length_From'])
    
   
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    
  
   
    id_value=db.query('select max("id") from "SMB"."SMB - Extra - Length Production - MiniBar"')
    id_value=(id_value[0][0]+1)
   
    input_tuple=(id_value, username, date_time,BusinessCode,Customer_Group,Market_Customer,Market_Country,Delivering_Mill,Length,Length_From,Length_To,Document_Item_Currency, Amount, Currency.strip("'"))
    
    query='''INSERT INTO "SMB"."SMB - Extra - Length Production - MiniBar"(
         "id",
         "Username",
         "date_time",
         "BusinessCode",
         "Customer Group",
       "Market - Customer",
         
       "Market - Country", 
       "Delivering Mill", 
       "Length",
       "Length From",
       "Length To",
         "Document Item Currency",
         "Amount",
         "Currency")
         VALUES{};'''.format(input_tuple)
    print(query)
         
   
    db.insert(query)
    return {"status":"success"},200

   
@smb_app3.route('/upload_length_production_minibar', methods=['GET','POST'])
def upload_length_production_minibar():
    
    f=request.files['filename']
    try:
            
        f.save(input_directory+f.filename)
        smb_df=pd.read_excel(input_directory+f.filename)
        
        df=smb_df[['id', 'Username', 'date_time', 'BusinessCode', 'Customer Group',
       'Market - Customer', 'Market - Country', 'Delivering Mill', 'Length ',
       'Length From', 'Length To', 'Document Item Currency', 'Amount',
       'Currency']]  
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
        table=json.loads(df.to_json(orient='records'))
       
        return {"data":table},200
    except:
        return {"statuscode":500,"message":"incorrect"},500


@smb_app3.route('/validate_length_production_minibar', methods=['GET','POST'])
def  validate_length_production_minibar():
    
        
        json_data=json.loads(request.data)
        
        try:
            df=pd.DataFrame(json_data["billet"])  
            
            username = getpass.getuser()
            now = datetime.now()
            date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
            
            id_value=db.query('select max("id") from "SMB"."SMB - Extra - Length Production - MiniBar"')
            id_value=(id_value[0][0]+1)
            
            df.columns = df.columns.str.replace('_', ' ')
            df.rename(columns={"Market Country":"Market - Country","Market Customer":"Market - Customer"},inplace=True)
            
            
            
            df.insert(0, 'id', range(id_value, id_value + len(df)))
            df.insert(1,'Username',username)
            df.insert(2,'date_time',date_time)
            
            df['id']=df['id'].astype(int)
            df['date_time']=pd.to_datetime(df['date_time'])
            
            df.to_sql("SMB - Extra - Length Production - MiniBar",con=engine, schema='SMB',if_exists='append', index=False)
            
            return {"status":"success"},200
        except:
            return {"status":"failure"},500
        
         
@smb_app3.route('/download_length_production_minibar',methods=['GET'])
def download_length_production_minibar():
   
        now = datetime.now()
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Length Production - MiniBar" where extract(month from "date_time")=extract(month from now()) order by "id" desc''', con=con)
        df.drop(['Username','date_time','id'],axis=1,inplace=True)
        df.to_excel('C:/Users/Administrator/Downloads/'+now.strftime("%d-%m-%Y-%H-%M-%S") +'bse_price.xlsx',index=False)
        return {"status":"success"},200





# *****************************************************************************************************************************************
# "SMB"."SMB - Extra - Length Logistic"


@smb_app3.route('/data_length_logistic',methods=['GET','POST'])
def  data_length_logistic():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    query='''select max("date_time") from "SMB"."SMB - Extra - Length Logistic"'''
    
    date_time_raw=db.query(query)
    date_time= date_time_raw[0][0].strftime("%m/%d/%Y, %H:%M:%S")
    
    try:
        search_string=int(search_string)
    except:
        search_string=search_string   
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Length Logistic" where extract(month from "date_time")=extract(month from now()) order by "id" desc OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Length Logistic"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count,"date_time":date_time},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app3.route('/delete_record_length_logistic',methods=['POST','GET','DELETE'])
def delete_record_length_logistic():  
    id_value=request.args.get('id')
    try:
        query='''delete  from "SMB"."SMB - Extra - Length Logistic" where "id"={}'''.format(id_value)
        db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app3.route('/get_record_length_logistic',methods=['GET','POST'])       
def get_record_length_logistic():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Length Logistic" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app3.route('/add_record_length_logistic',methods=['POST'])
def add_record_length_logistic():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    Country_Group=(query_parameters['Country_Group'])
    Market_Country=(query_parameters['Market_Country'])
    Delivering_Mill=(query_parameters['Delivering_Mill'])
    
    Length=(query_parameters['Length'])
    Length_From=(query_parameters['Length_From'])
    Length_To=(query_parameters['Length_From'])
    Transport_Mode=(query_parameters['Transport_Mode'])
    
   
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    
  
   
    id_value=db.query('select max("id") from "SMB"."SMB - Extra - Length Logistic"')
    id_value=(id_value[0][0]+1)
   
    input_tuple=(id_value, username, date_time,Country_Group,Market_Country,Delivering_Mill,Length,Length_From,Length_To,Transport_Mode,Document_Item_Currency, Amount, Currency.strip("'"))
    
    query='''INSERT INTO "SMB"."SMB - Extra - Length Logistic"(
         "id",
         "Username",
         "date_time",
         "Country Group",
         "Market - Country",
       "Delivering Mill",
       "Length",
       "Length From",
       "Length To",
       "Transport Mode",
         "Document Item Currency",
         "Amount",
         "Currency")
         VALUES{};'''.format(input_tuple)
    print(query)
         
   
    db.insert(query)
    return {"status":"success"},200

   
@smb_app3.route('/upload_length_logistic', methods=['GET','POST'])
def upload_length_logistic():
    
    f=request.files['filename']
    try:
            
        f.save(input_directory+f.filename)
        smb_df=pd.read_excel(input_directory+f.filename)
        
        df=smb_df[['id', 'Username', 'date_time', 'Country Group', 'Market - Country',
       'Delivering Mill', 'Length', 'Length From', 'Length To',
       'Transport Mode', 'Document Item Currency', 'Amount', 'Currency']]  
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        table=json.loads(df.to_json(orient='records'))
       
        return {"data":table},200
    except:
        return {"statuscode":500,"message":"incorrect"},500


@smb_app3.route('/validate_length_logistic', methods=['GET','POST'])
def  validate_length_logistic():
    
        
        json_data=json.loads(request.data)
        
        try:
            df=pd.DataFrame(json_data["billet"])  
            
            username = getpass.getuser()
            now = datetime.now()
            date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
            
            id_value=db.query('select max("id") from "SMB"."SMB - Extra - Length Logistic"')
            id_value=(id_value[0][0]+1)
            
            df.columns = df.columns.str.replace('_', ' ')
            df.rename(columns={"Market Country":"Market - Country"},inplace=True)
            
            
            
            df.insert(0, 'id', range(id_value, id_value + len(df)))
            df.insert(1,'Username',username)
            df.insert(2,'date_time',date_time)
            
            df['id']=df['id'].astype(int)
            df['date_time']=pd.to_datetime(df['date_time'])
            
            df.to_sql("SMB - Extra - Length Logistic",con=engine, schema='SMB',if_exists='append', index=False)
            
            return {"status":"success"},200
        except:
            return {"status":"failure"},500
        
         
@smb_app3.route('/download_length_logistic',methods=['GET'])
def download_length_logistic():
   
        now = datetime.now()
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Length Logistic" where extract(month from "date_time")=extract(month from now()) order by "id" desc''', con=con)
        df.drop(['Username','date_time','id'],axis=1,inplace=True)
        df.to_excel('C:/Users/Administrator/Downloads/'+now.strftime("%d-%m-%Y-%H-%M-%S") +'bse_price.xlsx',index=False)
        return {"status":"success"},200




# *****************************************************************************************************************************************
# "SMB"."SMB - Extra - Length Logistic Minibar"


@smb_app3.route('/data_length_logistic_minibar',methods=['GET','POST'])
def  data_length_logistic_minibar():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    query='''select max("date_time") from "SMB"."SMB - Extra - Length Logistic - MiniBar"'''
    
    date_time_raw=db.query(query)
    date_time= date_time_raw[0][0].strftime("%m/%d/%Y, %H:%M:%S")
    
    try:
        search_string=int(search_string)
    except:
        search_string=search_string   
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Length Logistic - MiniBar" where extract(month from "date_time")=extract(month from now()) order by "id" desc OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Length Logistic - MiniBar"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)  
       
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count,"date_time":date_time},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app3.route('/delete_record_length_logistic_minibar',methods=['POST','GET','DELETE'])
def delete_record_length_logistic_minibar():  
    id_value=request.args.get('id')
    try:
        query='''delete  from "SMB"."SMB - Extra - Length Logistic - MiniBar" where "id"={}'''.format(id_value)
        db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app3.route('/get_record_length_logistic_minibar',methods=['GET','POST'])       
def get_record_length_logistic_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Length Logistic - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)  
       
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app3.route('/add_record_length_logistic_minibar',methods=['POST'])
def add_record_length_logistic_minibar():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    Customer_Group=(query_parameters['Customer_Group'])
    Market_Customer=(query_parameters['Market_Customer'])
    
    
    Market_Country=(query_parameters['Market_Country'])
    Delivering_Mill=(query_parameters['Delivering_Mill'])
    
    Length=(query_parameters['Length'])
    Length_From=(query_parameters['Length_From'])
    Length_To=(query_parameters['Length_From'])
    Transport_Mode=(query_parameters['Transport_Mode'])
    
   
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    
  
   
    id_value=db.query('select max("id") from "SMB"."SMB - Extra - Length Logistic - MiniBar"')
    id_value=(id_value[0][0]+1)
   
    input_tuple=(id_value, username, date_time,Customer_Group,Market_Customer,Market_Country,Delivering_Mill,Length,Length_From,Length_To,Transport_Mode,Document_Item_Currency, Amount, Currency.strip("'"))
    
    query='''INSERT INTO "SMB"."SMB - Extra - Length Logistic - MiniBar"(
         "id",
         "Username",
         "date_time",
         "Customer Group",
         "Market - Customer",
       "Market - Country", 
       "Delivering Mill",
       "Length",
       "Length From",
       "Length To",
       "Transport Mode",
         "Document Item Currency",
         "Amount",
         "Currency")
         VALUES{};'''.format(input_tuple)
    print(query)
         
   
    db.insert(query)
    return {"status":"success"},200

   
@smb_app3.route('/upload_length_logistic_minibar', methods=['GET','POST'])
def upload_length_logistic_minibar():
    
    f=request.files['filename']
    try:
            
        f.save(input_directory+f.filename)
        smb_df=pd.read_excel(input_directory+f.filename)
        
        df=smb_df[['id', 'Username', 'date_time', 'Customer Group', 'Market - Customer',
       'Market - Country', 'Delivering Mill', 'Length', 'Length From',
       'Length To', 'Transport Mode', 'Document Item Currency', 'Amount',
       'Currency']]  
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)  
        table=json.loads(df.to_json(orient='records'))
       
        return {"data":table},200
    except:
        return {"statuscode":500,"message":"incorrect"},500


@smb_app3.route('/validate_length_logistic_minibar', methods=['GET','POST'])
def  validate_length_logistic_minibar():
    
        
        json_data=json.loads(request.data)
        
        try:
            df=pd.DataFrame(json_data["billet"])  
            
            username = getpass.getuser()
            now = datetime.now()
            date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
            
            id_value=db.query('select max("id") from "SMB"."SMB - Extra - Length Logistic - MiniBar"')
            id_value=(id_value[0][0]+1)
            
            df.columns = df.columns.str.replace('_', ' ')
            df.rename(columns={"Market Country":"Market - Country","Market Customer":"Market - Customer"},inplace=True)
            
            
            
            df.insert(0, 'id', range(id_value, id_value + len(df)))
            df.insert(1,'Username',username)
            df.insert(2,'date_time',date_time)
            
            df['id']=df['id'].astype(int)
            df['date_time']=pd.to_datetime(df['date_time'])
            
            df.to_sql("SMB - Extra - Length Logistic - MiniBar",con=engine, schema='SMB',if_exists='append', index=False)
            
            return {"status":"success"},200
        except:
            return {"status":"failure"},500
        
         
@smb_app3.route('/download_length_logistic_minibar',methods=['GET'])
def download_length_logistic_minibar():
   
        now = datetime.now()
        df = pd.read_sql('''select *  from "SMB"."SMB - Extra - Length Logistic - MiniBar" where extract(month from "date_time")=extract(month from now()) order by "id" desc''', con=con)
        df.drop(['Username','date_time','id'],axis=1,inplace=True)
        df.to_excel('C:/Users/Administrator/Downloads/'+now.strftime("%d-%m-%Y-%H-%M-%S") +'bse_price.xlsx',index=False)
        return {"status":"success"},200






