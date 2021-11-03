# -*- coding: utf-8 -*-
"""
Created on Sun Oct 24 07:12:53 2021

@author: Administrator
"""


from flask import Blueprint


import pandas as pd
import time
import json
from flask import Flask, request, send_file, render_template
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


download_path='C:/SMB/smb_download/'

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
    
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Transport Mode" where "active"='1' order by "id"  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Transport Mode"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app3.route('/delete_record_transport',methods=['POST','GET','DELETE'])
def delete_record_transport():  
    id_value=request.args.get('id')
    try:
        query='''UPDATE "SMB"."SMB - Extra - Transport Mode" SET "active"=0 WHERE "id"={} '''.format(id_value)
        result=db.insert(query)
        
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
    
    try:
   
        input_tuple=( username,Product_Division,Market_Country,Transport_Mode,Document_Item_Currency, Amount, Currency.strip("'"))
        
        query='''INSERT INTO "SMB"."SMB - Extra - Transport Mode"(
            
             "Username",
             
             "Product Division", 
             "Market - Country",
           "Transport Mode",
           
          
             "Document Item Currency",
             "Amount",
             "Currency")
             VALUES{};'''.format(input_tuple)
        print(query)
             
       
        result=db.insert(query)
        if result=='failed':raise ValueError
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
    

@smb_app3.route('/update_record_transport',methods=['POST'])
def update_record_transport():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    Product_Division =( query_parameters["Product_Division"])
    Market_Country=(query_parameters['Market_Country'])
    Transport_Mode=(query_parameters["Transport_Mode"])
    id_value=(query_parameters['id'])
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    try:
        
        query1='''INSERT INTO "SMB"."SMB - Extra - Transport Mode_History"
        SELECT 
        "id","Username",now(),"Product Division", "Market - Country",
       "Transport Mode", "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Transport Mode"
        WHERE "id"={} '''.format(id_value)
        result=db.insert(query1)
        if result=='failed' :raise ValueError
    
        query2='''UPDATE "SMB"."SMB - Extra - Transport Mode"
        SET 
       "Username"='{0}',
       "Product Division"='{1}',
       "Market - Country"='{2}',
      	  
       "Transport Mode"='{3}',
       "Document Item Currency"='{4}',
       "Amount"='{5}',
       "Currency"=''{6}'',
       "updated_on"='{7}'
        WHERE "id"={8} '''.format(username,Product_Division,Market_Country,Transport_Mode,Document_Item_Currency,Amount,Currency,date_time,id_value)
        result1=db.insert(query2)
        if result1=='failed' :raise ValueError
        print(query1)
        print("*****************************")
        print(query2)
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
     


   
@smb_app3.route('/upload_transport', methods=['GET','POST'])
def upload_transport():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","Product Division", "Market - Country",
       "Transport Mode", "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            df_main = pd.read_sql('''select "id","Product Division", "Market - Country",
       "Transport Mode", "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Transport Mode" where "active"='1' order by "id" ''', con=con)
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500


@smb_app3.route('/validate_transport', methods=['GET','POST'])
def  validate_transport():
    
        
    json_data=json.loads(request.data)
    username = getpass.getuser() 
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    df.insert(1,'date_time',date_time)
    try:
       
        df=df[ ["Username","Product_Division", "Market_Country",
       "Transport_Mode", "Document_Item_Currency", "Amount", "Currency","date_time","id"]]
        
        query1='''INSERT INTO "SMB"."SMB - Extra - Transport Mode_History"
        SELECT 
        "id",
        "Username",now(),
        "Product Division", "Market - Country",
       "Transport Mode", "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Transport Mode"
        WHERE "id" in {} '''
        
        id_tuple=tuple(df["id"])
        if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
        print(query1.format(id_tuple))
        result=db.insert(query1.format(id_tuple))
        if result=='failed' :raise ValueError
        
        
        # looping for update query
        for i in range(0,len(df)):
            print(tuple(df.loc[0]))
            query2='''UPDATE "SMB"."SMB - Extra - Transport Mode"
        SET 
       "Username"='%s',
       "Product Division"='%s', "Market - Country"='%s',
       "Transport Mode"='%s',
       
       "Document Item Currency"='%s',
       "Amount"='%s',
       "Currency"=''%s'',
       "updated_on"='%s'
        WHERE "id"= '%s' ''' % tuple(df.loc[i])
            print(query2)
            result=db.insert(query2)
            if result=='failed' :raise ValueError
            
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
         
@smb_app3.route('/download_transport',methods=['GET'])
def download_transport():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select * from "SMB"."SMB - Extra - Transport Mode" where "active"='1' order by "id" ''', con=con)
            df.drop(['Username','updated_on','active','aprover1','aprover2','aprover3'],axis=1,inplace=True)
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'extra_transport_mode.xlsx'
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
        except:
            return {"status":"failure"},500
   


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
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Transport Mode - MiniBar" where "active"='1' order by "id"  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Transport Mode - MiniBar"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
         
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app3.route('/delete_record_transport_minibar',methods=['POST','GET','DELETE'])
def delete_record_transport_minibar():  
    id_value=request.args.get('id')
    try:
        query='''UPDATE "SMB"."SMB - Extra - Transport Mode - MiniBar" SET "active"=0 WHERE "id"={} '''.format(id_value)
        result=db.insert(query)
        
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
    
    try:
        input_tuple=( username,Product_Division,Market_Country,Market_Customer_Group,Market_Customer_Group,Transport_Mode,Document_Item_Currency, Amount, Currency.strip("'"))
        
        query='''INSERT INTO "SMB"."SMB - Extra - Transport Mode - MiniBar"(
            
             "Username",
            
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
             
       
        result=db.insert(query)
        if result=='failed':raise ValueError
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
    


@smb_app3.route('/update_record_transport_minibar',methods=['POST'])
def update_record_transport_minibar():
    
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
        id_value=(query_parameters['id'])
        Document_Item_Currency =( query_parameters["Document_Item_Currency"])
        Amount =( query_parameters["Amount"])
        Currency =( query_parameters["Currency"])
        
   
        try:
            query1='''INSERT INTO "SMB"."SMB - Extra - Transport Mode - MiniBar_History" 
            SELECT 
            "id","Username",now(),"Product Division", "Market - Country",
           "Market - Customer Group", "Market - Customer", "Transport Mode",
           "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Transport Mode - MiniBar"
            WHERE "id"={} '''.format(id_value)
            result=db.insert(query1)
            if result=='failed' :raise ValueError
        
            query2='''UPDATE "SMB"."SMB - Extra - Transport Mode - MiniBar"
            SET 
           "Username"='{0}',
          "Product Division"='{1}',
           "Market - Country"='{2}',
           "Market - Customer Group"='{3}',
           "Market - Customer"='{4}',
           "Transport Mode"='{5}',
          	   
           "Document Item Currency"='{6}',
           "Amount"='{7}',
           "Currency"=''{8}'',
           "updated_on"='{9}'
            WHERE "id"={10} '''.format(username,Product_Division,Market_Country,Market_Customer_Group,Market_Customer,Transport_Mode,Document_Item_Currency,Amount,Currency,date_time,id_value)
            result1=db.insert(query2)
            if result1=='failed' :raise ValueError
            print(query1)
            print("*****************************")
            print(query2)
            return {"status":"success"},200
        except :
            return {"status":"failure"},500
        
        
    

   
@smb_app3.route('/upload_transport_minibar', methods=['GET','POST'])
def upload_transport_minibar():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","Product Division", "Market - Country",
       "Market - Customer Group", "Market - Customer", "Transport Mode",
       "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            df_main = pd.read_sql('''select "id","Product Division", "Market - Country",
       "Market - Customer Group", "Market - Customer", "Transport Mode",
       "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Transport Mode - MiniBar" where "active"='1' order by "id" ''', con=con)
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)  
         
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500


@smb_app3.route('/validate_transport_minibar', methods=['GET','POST'])
def  validate_transport_minibar():
    
        
    json_data=json.loads(request.data)
    username = getpass.getuser() 
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    df.insert(1,'date_time',date_time)
    try:
       
        df=df[ ["Username","Product_Division", "Market_Country",
       "Market_Customer_Group", "Market_Customer", "Transport_Mode",
       "Document_Item_Currency", "Amount", "Currency","date_time","id"]]
        
        query1='''INSERT INTO "SMB"."SMB - Extra - Transport Mode - MiniBar_History"
        SELECT 
        "id",
        "Username",now(),
        "Product Division", "Market - Country",
       "Market - Customer Group", "Market - Customer", "Transport Mode",
       "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Transport Mode - MiniBar"
        WHERE "id" in {} '''
        
        id_tuple=tuple(df["id"])
        if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
        result=db.insert(query1.format(id_tuple))
        if result=='failed' :raise ValueError
        
        # looping for update query
        for i in range(0,len(df)):
            print(tuple(df.loc[0]))
            query2='''UPDATE "SMB"."SMB - Extra - Transport Mode - MiniBar"
        SET 
       "Username"='%s',
       "Product Division"='%s', "Market - Country"='%s',
       "Market - Customer Group"='%s',
       
      "Market - Customer"='%s', "Transport Mode"='%s',
       
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
        
         
@smb_app3.route('/download_transport_minibar',methods=['GET'])
def download_transport_minibar():
        now = datetime.now()
        try:
            df = pd.read_sql('''select * from "SMB"."SMB - Extra - Transport Mode - MiniBar" where "active"='1' order by "id" ''', con=con)
            df.drop(['Username','updated_on','active','aprover1','aprover2','aprover3'],axis=1,inplace=True)
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'transport_mode_minibar.xlsx'
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
        except:
            return {"status":"failure"},500
   
      

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
   
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Length Production" where "active"='1' order by "id"  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Length Production"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app3.route('/delete_record_length_production',methods=['POST','GET','DELETE'])
def delete_record_length_production():  
    id_value=request.args.get('id')
    try:
        query='''UPDATE "SMB"."SMB - Extra - Length Production" SET "active"=0 WHERE "id"={} '''.format(id_value)
        result=db.insert(query)
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
    
    
  
   
    
    try:
        input_tuple=(username,BusinessCode,Country_Group,Market_Country,Delivering_Mill,Length,Length_From,Length_To,Document_Item_Currency, Amount, Currency.strip("'"))
        
        query='''INSERT INTO "SMB"."SMB - Extra - Length Production"(
             
             "Username",
            
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
             
       
        result=db.insert(query)
        if result== 'failed':raise ValueError
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@smb_app3.route('/update_record_length_production',methods=['POST'])
def update_record_length_production():
    
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
    id_value=(query_parameters['id'])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    try:
        
        query1='''INSERT INTO "SMB"."SMB - Extra - Length Production_History"
        SELECT 
        "id","Username",now(),"BusinessCode", "Country Group",
       "Market - Country", "Delivering Mill", "Length", "Length From",
       "Length To", "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Length Production"
        WHERE "id"={} '''.format(id_value)
        result=db.insert(query1)
        print(query1)
        if result=='failed' :raise ValueError
    
        query2='''UPDATE "SMB"."SMB - Extra - Length Production"
        SET 
       "Username"='{0}',
       "BusinessCode"='{1}',
       "Country Group"='{2}',
       "Market - Country"='{3}',
      	   "Delivering Mill"='{4}',
             "Length"='{5}', "Length From"='{6}',
       "Length To"='{7}', 
       
       "Document Item Currency"='{8}',
       "Amount"='{9}',
       "Currency"=''{10}'',
       "updated_on"='{11}'
        WHERE "id"={12} '''.format(username,BusinessCode,Country_Group,Market_Country,Delivering_Mill,Length,Length_From ,Length_To,Document_Item_Currency,Amount,Currency,date_time,id_value)
        result1=db.insert(query2)
        print(query2)
        if result1=='failed' :raise ValueError
       
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
     

   
@smb_app3.route('/upload_length_production', methods=['GET','POST'])
def upload_length_production():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","BusinessCode", "Country Group",
       "Market - Country", "Delivering Mill", "Length", "Length From",
       "Length To", "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Country Group",
       "Market - Country", "Delivering Mill", "Length", "Length From",
       "Length To", "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Length Production" where "active"='1' order by "id" ''', con=con)
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500

@smb_app3.route('/validate_length_production', methods=['GET','POST'])
def  validate_length_production():
    
        
    json_data=json.loads(request.data)
    username = getpass.getuser() 
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    df.insert(1,'date_time',date_time)
    try:
       
        df=df[ ["Username","BusinessCode", "Country_Group",
       "Market_Country", "Delivering_Mill", "Length", "Length_From",
       "Length_To", "Document_Item_Currency", "Amount", "Currency","date_time","id"]]
        
        query1='''INSERT INTO "SMB"."SMB - Extra - Length Production_History"
        SELECT 
        "id",
        "Username",now(),
        "BusinessCode", "Country Group",
       "Market - Country", "Delivering Mill", "Length", "Length From",
       "Length To", "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Length Production"
        WHERE "id" in {} '''
        
        id_tuple=tuple(df["id"])
        if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
        result=db.insert(query1.format(id_tuple))
        if result=='failed' :raise ValueError
        
        
        # looping for update query
        for i in range(0,len(df)):
            print(tuple(df.loc[0]))
            query2='''UPDATE "SMB"."SMB - Extra - Length Production"
        SET 
       "Username"='%s',
       "BusinessCode"='%s',  "Country Group"='%s',"Market - Country"='%s',
       "Delivering Mill"='%s',"Length"='%s',"Length From"='%s',"Length To"='%s',
       
       "Document Item Currency"='%s',
       "Amount"='%s',
       "Currency"=''%s'',
       "updated_on"='%s'
        WHERE "id"= '%s' ''' % tuple(df.loc[i])
            print(query2)
            result=db.insert(query2)
            if result=='failed' :raise ValueError
            
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
        
         
@smb_app3.route('/download_length_production',methods=['GET'])
def download_length_production():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select * from "SMB"."SMB - Extra - Length Production" where "active"='1' order by "id" ''', con=con)
            df.drop(['Username','updated_on','active','aprover1','aprover2','aprover3'],axis=1,inplace=True)
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'length_production_minibar.xlsx'
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
        except:
            return {"status":"failure"},500




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
    
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Length Production - MiniBar" where "active"='1' order by "id"  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Length Production - MiniBar"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app3.route('/delete_record_length_production_minibar',methods=['POST','GET','DELETE'])
def delete_record_length_production_minibar():  
    id_value=request.args.get('id')
    try:
        query='''UPDATE "SMB"."SMB - Extra - Length Production - MiniBar" SET "active"=0 WHERE "id"={} '''.format(id_value)
        result=db.insert(query)
        
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
    
    try:
        input_tuple=( username,BusinessCode,Customer_Group,Market_Customer,Market_Country,Delivering_Mill,Length,Length_From,Length_To,Document_Item_Currency, Amount, Currency.strip("'"))
        
        query='''INSERT INTO "SMB"."SMB - Extra - Length Production - MiniBar"(
             
             "Username",
            
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
             
       
        result=db.insert(query)
        if result=='failed':raise ValueError
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
   


@smb_app3.route('/update_record_length_production_minibar',methods=['POST'])
def update_record_length_production_minibar():
    
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
    id_value=(query_parameters['id'])
    
    try:
        
        query1='''INSERT INTO "SMB"."SMB - Extra - Length Production - MiniBar_History"
        SELECT 
        "id","Username",now(),"BusinessCode", "Customer Group",
       "Market - Customer", "Market - Country", "Delivering Mill", "Length",
       "Length From", "Length To", "Document Item Currency", "Amount",
       "Currency" FROM "SMB"."SMB - Extra - Length Production - MiniBar"
        WHERE "id"={} '''.format(id_value)
        result=db.insert(query1)
        print(query1)
        if result=='failed' :raise ValueError
    
        query2='''UPDATE "SMB"."SMB - Extra - Length Production - MiniBar"
        SET 
       "Username"='{0}',
       "BusinessCode"='{1}',
       "Customer Group"='{2}',
       "Market - Customer"='{3}',
       "Market - Country"='{4}',
      	  "Delivering Mill"='{5}',
       "Length"='{6}',
       "Length From"='{7}', "Length To"='{8}',
       "Document Item Currency"='{9}',
       "Amount"='{10}',
       "Currency"=''{11}'',
       "updated_on"='{12}'
        WHERE "id"={13} '''.format(username,BusinessCode,Customer_Group,Market_Customer,Market_Country,Delivering_Mill,Length,Length_From,Length_To,Document_Item_Currency,Amount,Currency,date_time,id_value)
        result1=db.insert(query2)
        print(query2)
        if result1=='failed' :raise ValueError
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
     

       
    

@smb_app3.route('/upload_length_production_minibar', methods=['GET','POST'])
def upload_length_production_minibar():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","BusinessCode", "Customer Group",
       "Market - Customer", "Market - Country", "Delivering Mill", "Length",
       "Length From", "Length To", "Document Item Currency", "Amount",
       "Currency"]]  
            df["id"]=df["id"].astype(int)
            print(df.columns)
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Customer Group",
       "Market - Customer", "Market - Country", "Delivering Mill", "Length",
       "Length From", "Length To", "Document Item Currency", "Amount",
       "Currency" from "SMB"."SMB - Extra - Length Production - MiniBar" where "active"='1' order by "id" ''', con=con)
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
        
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500
    


@smb_app3.route('/validate_length_production_minibar', methods=['GET','POST'])
def  validate_length_production_minibar():
    
        
    json_data=json.loads(request.data)
    username = getpass.getuser() 
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    df.insert(1,'date_time',date_time)
    try:
       
        df=df[ ["Username","BusinessCode", "Customer_Group",
       "Market_Customer", "Market_Country", "Delivering_Mill", "Length",
       "Length_From", "Length_To", "Document_Item_Currency", "Amount",
       "Currency","date_time","id"]]
        
        query1='''INSERT INTO "SMB"."SMB - Extra - Length Production - MiniBar_History"
        SELECT 
        "id",
        "Username",now(),
        "BusinessCode", "Customer Group",
       "Market - Customer", "Market - Country", "Delivering Mill", "Length",
       "Length From", "Length To", "Document Item Currency", "Amount",
       "Currency"  FROM "SMB"."SMB - Extra - Length Production - MiniBar"
        WHERE "id" in {} '''
        
        id_tuple=tuple(df["id"])
        if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
        print(query1.format(id_tuple))
        result=db.insert(query1.format(id_tuple))
        if result=='failed' :raise ValueError
        
        
        # looping for update query
        for i in range(0,len(df)):
            print(tuple(df.loc[0]))
            query2='''UPDATE "SMB"."SMB - Extra - Length Production - MiniBar"
        SET 
       "Username"='%s',
       "BusinessCode"='%s',  "Customer Group"='%s',"Market - Customer"='%s',"Market - Country"='%s',
       "Delivering Mill"='%s',"Length"='%s',"Length From"='%s',"Length To"='%s',
       
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
         
@smb_app3.route('/download_length_production_minibar',methods=['GET'])
def download_length_production_minibar():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select * from "SMB"."SMB - Extra - Length Production - MiniBar" where "active"='1' order by "id" ''', con=con)
            df.drop(['Username','updated_on','active','aprover1','aprover2','aprover3'],axis=1,inplace=True)
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'length_production_minibar.xlsx'
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
        except:
            return {"status":"failure"},500





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
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Length Logistic" where "active"='1' order by "id"  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Length Logistic"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app3.route('/delete_record_length_logistic',methods=['POST','GET','DELETE'])
def delete_record_length_logistic():  
    id_value=request.args.get('id')
    try:
        query='''UPDATE "SMB"."SMB - Extra - Length Logistic" SET "active"=0 WHERE "id"={} '''.format(id_value)
        result=db.insert(query)
        
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
    
    
  
    try:
        
   
        input_tuple=( username,Country_Group,Market_Country,Delivering_Mill,Length,Length_From,Length_To,Transport_Mode,Document_Item_Currency, Amount, Currency.strip("'"))
        
        query='''INSERT INTO "SMB"."SMB - Extra - Length Logistic"(
            
             "Username",
             
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
    except:
        return {"status":"failure"},500



@smb_app3.route('/update_record_length_logistic',methods=['POST'])
def update_record_length_logistic():
    
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
    
    id_value=(query_parameters['id'])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    
    try:
        
        query1='''INSERT INTO "SMB"."SMB - Extra - Length Logistic_History" 
        SELECT 
        "id","Username",now(),"Country Group", "Market - Country",
       "Delivering Mill", "Length", "Length From", "Length To",
       "Transport Mode", "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Length Logistic"
        WHERE "id"={} '''.format(id_value)
        result=db.insert(query1)
        if result=='failed' :raise ValueError
    
        query2='''UPDATE "SMB"."SMB - Extra - Length Logistic"
        SET 
       "Username"='{0}',
       "Country Group"='{1}',
       "Market - Country"='{2}',
      	   "Delivering Mill"='{3}',
       "Length"='{4}',
       "Length From"='{5}',
       "Length To"='{6}',
       "Transport Mode"='{7}',
       "Document Item Currency"='{8}',
       "Amount"='{9}',
       "Currency"=''{10}'',
       "updated_on"='{11}'
        WHERE "id"={12} '''.format(username,Country_Group,Market_Country,Delivering_Mill,Length,Length_From,Length_To,Transport_Mode,Document_Item_Currency,Amount,Currency,date_time,id_value)
        result1=db.insert(query2)
        if result1=='failed' :raise ValueError
        print(query1)
        print("*****************************")
        print(query2)
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
     

    
   
@smb_app3.route('/upload_length_logistic', methods=['GET','POST'])
def upload_length_logistic():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","Country Group", "Market - Country",
       "Delivering Mill", "Length", "Length From", "Length To",
       "Transport Mode", "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            df_main = pd.read_sql('''select "id","Country Group", "Market - Country",
       "Delivering Mill", "Length", "Length From", "Length To",
       "Transport Mode", "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Length Logistic" where "active"='1' order by "id" ''', con=con)
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500


@smb_app3.route('/validate_length_logistic', methods=['GET','POST'])
def  validate_length_logistic():
    
         json_data=json.loads(request.data)
         username = getpass.getuser() 
         now = datetime.now()
         date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
        
         df=pd.DataFrame(json_data["billet"]) 
        
         df.insert(0,'Username',username)
         df.insert(1,'date_time',date_time)
    
       
         df=df[ ["Username","Country_Group", "Market_Country",
        "Delivering_Mill", "Length", "Length_From", "Length_To",
        "Transport_Mode", "Document_Item_Currency", "Amount", "Currency","date_time","id"]]
         
         query1='''INSERT INTO "SMB"."SMB - Extra - Length Logistic_History" 
         SELECT 
         "id",
         "Username",now(),
         "Country Group", "Market - Country",
        "Delivering Mill", "Length", "Length From", "Length To",
        "Transport Mode", "Document Item Currency", "Amount", "Currency"  FROM "SMB"."SMB - Extra - Length Logistic"
         WHERE "id" in {} '''
         
         id_tuple=tuple(df["id"])
         if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
         result=db.insert(query1.format(id_tuple))
         if result=='failed' :raise ValueError
         
         # looping for update query
         for i in range(0,len(df)):
             print(tuple(df.loc[0]))
             query2='''UPDATE "SMB"."SMB - Extra - Length Logistic"
         SET 
        "Username"='%s',
        "Country Group"='%s', "Market - Country"='%s',
        "Delivering Mill"='%s',"Length"='%s', "Length From"='%s', "Length To"='%s',
        "Transport Mode"='%s',
        "Document Item Currency"='%s',
        "Amount"='%s',
        "Currency"=''%s'',
        "updated_on"='%s'
         WHERE "id"= '%s' ''' % tuple(df.loc[i])
             result=db.insert(query2)
             if result=='failed' :raise ValueError
             
         return {"status":"success"},200
          
          
@smb_app3.route('/download_length_logistic',methods=['GET'])
def download_length_logistic():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select * from "SMB"."SMB - Extra - Length Logistic" where "active"='1' order by "id" ''', con=con)
            df.drop(['Username','updated_on','active','aprover1','aprover2','aprover3'],axis=1,inplace=True)
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'length_logistic_minibar.xlsx'
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
        except:
            return {"status":"failure"},500





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
    
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Length Logistic - MiniBar" where "active"='1' order by "id"  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Length Logistic - MiniBar"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)  
       
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app3.route('/delete_record_length_logistic_minibar',methods=['POST','GET','DELETE'])
def delete_record_length_logistic_minibar():  
    id_value=request.args.get('id')
    try:
        query='''UPDATE "SMB"."SMB - Extra - Length Logistic - MiniBar" SET "active"=0 WHERE "id"={} '''.format(id_value)
        result=db.insert(query)
        
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
    
    
    try:
        
        input_tuple=( username,Market_Customer,Market_Country,Delivering_Mill,Length,Length_From,Length_To,Transport_Mode,Document_Item_Currency, Amount, Currency.strip("'"))
        
        query='''INSERT INTO "SMB"."SMB - Extra - Length Logistic - MiniBar"(
             
             "Username",
             
             
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
             
       
        result=db.insert(query)
        if result=='failed': raise ValueError
        return {"status":"success"},200
    except:
        return {"status":"failure"},500



@smb_app3.route('/update_record_length_logistic_minibar',methods=['POST'])
def update_record_length_logistic_minibar():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    
    Market_Customer=(query_parameters['Market_Customer'])
    
    Market_Country=(query_parameters['Market_Country'])
    Delivering_Mill=(query_parameters['Delivering_Mill'])
    
    Length=(query_parameters['Length'])
    Length_From=(query_parameters['Length_From'])
    Length_To=(query_parameters['Length_From'])
    Transport_Mode=(query_parameters['Transport_Mode'])
    Customer_Group=(query_parameters['Customer_Group'])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    id_value=(query_parameters['id'])
    
    try:
        
        query1='''INSERT INTO "SMB"."SMB - Extra - Length Logistic - MiniBar_History"
        SELECT 
        "id","Username",now(),"Customer Group", "Market - Customer",
       "Market - Country", "Delivering Mill", "Length", "Length From",
       "Length To", "Transport Mode", "Document Item Currency", "Amount",
       "Currency"  FROM "SMB"."SMB - Extra - Length Logistic - MiniBar"
        WHERE "id"={} '''.format(id_value)
        result=db.insert(query1)
        if result=='failed' :raise ValueError
    
        query2='''UPDATE "SMB"."SMB - Extra - Length Logistic - MiniBar"
        SET 
       "Username"='{0}',
       "Customer Group"='{1}',
       "Market - Customer"='{2}',
       "Market - Country"='{3}',
      	   "Delivering Mill"='{4}',
             "Length"='{5}', "Length From"='{6}',
       "Length To"='{7}',
        "Transport Mode"='{8}',
      
       "Document Item Currency"='{9}',
       "Amount"='{10}',
       "Currency"=''{11}'',
       "updated_on"='{12}'
        WHERE "id"={13} '''.format(username,Customer_Group,Market_Customer,Market_Country,Delivering_Mill,Length,Length_From,Length_To,Transport_Mode,Document_Item_Currency,Amount,Currency,date_time,id_value)
        result1=db.insert(query2)
        if result1=='failed' :raise ValueError
        print(query1)
        print("*****************************")
        print(query2)
        return {"status":"success"},200
    except:
        return {"status":"failure"},500
     

    
   
@smb_app3.route('/upload_length_logistic_minibar', methods=['GET','POST'])
def upload_length_logistic_minibar():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","Customer Group", "Market - Customer",
       "Market - Country", "Delivering Mill", "Length", "Length From",
       "Length To", "Transport Mode", "Document Item Currency", "Amount",
       "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            df_main = pd.read_sql('''select "id","Customer Group", "Market - Customer",
       "Market - Country", "Delivering Mill", "Length", "Length From",
       "Length To", "Transport Mode", "Document Item Currency", "Amount",
       "Currency" from "SMB"."SMB - Extra - Length Logistic - MiniBar" where "active"='1' order by "id" ''', con=con)
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)  
       
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500
    


@smb_app3.route('/validate_length_logistic_minibar', methods=['GET','POST'])
def  validate_length_logistic_minibar():
    
        
    json_data=json.loads(request.data)
    username = getpass.getuser() 
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    df.insert(1,'date_time',date_time)
    try:
       
        df=df[ ["Username","Customer_Group", "Market_Customer",
       "Market_Country", "Delivering_Mill", "Length", "Length_From",
       "Length_To", "Transport_Mode", "Document_Item_Currency", "Amount",
       "Currency","date_time","id"]]
        
        query1='''INSERT INTO "SMB"."SMB - Extra - Length Logistic - MiniBar_History"
        SELECT 
        "id",
        "Username",now(),
        "Customer Group", "Market - Customer",
       "Market - Country", "Delivering Mill", "Length", "Length From",
       "Length To", "Transport Mode", "Document Item Currency", "Amount",
       "Currency"  FROM "SMB"."SMB - Extra - Length Logistic - MiniBar"
        WHERE "id" in {} '''
        
        id_tuple=tuple(df["id"])
        if len(id_tuple)==1:id=id_tuple[0] ;id_tuple=(id,id)
        result=db.insert(query1.format(id_tuple))
        if result=='failed' :raise ValueError
        
        
        # looping for update query
        for i in range(0,len(df)):
            print(tuple(df.loc[0]))
            query2='''UPDATE "SMB"."SMB - Extra - Length Logistic - MiniBar"
        SET 
       "Username"='%s',
       "Customer Group"='%s',"Market - Customer"='%s', "Market - Country"='%s',
       "Delivering Mill"='%s', "Length"='%s', "Length From"='%s',
       "Length To"='%s', "Transport Mode"='%s',
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
        
         
@smb_app3.route('/download_length_logistic_minibar',methods=['GET'])
def download_length_logistic_minibar():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select * from "SMB"."SMB - Extra - Length Logistic - MiniBar" where "active"='1' order by "id" ''', con=con)
            df.drop(['Username','updated_on','active','aprover1','aprover2','aprover3'],axis=1,inplace=True)
            df.to_excel('C:/Users/Administrator/Downloads/'+now.strftime("%d-%m-%Y-%H-%M-%S") +'bse_price_extra_length_logistic_minibar.xlsx',index=False)
            return {"status":"success"},200
        except:
            return {"status":"failure"},500





