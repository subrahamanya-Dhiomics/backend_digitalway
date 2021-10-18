# -*- coding: utf-8 -*-
"""
Created on Thu Oct  12 07:33:38 2021

@author: subbu

"""

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
               
 

app = Flask(__name__)
CORS(app)
db=Database()

input_directory="C:/Users/Administrator/Documents/SMB_INPUT/"
con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')




@app.route('/Base_Price_Data',methods=['GET','POST'])
def SMB_data():
    # query_paramters 
    search_string=request.args.get("search_string")
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    query='''select max("date_time") from "SMB"."SMB - Base Price - Category Addition"'''
    date_time_raw=db.query(query)
    date_time= date_time_raw[0][0].strftime("%m/%d/%Y, %H:%M:%S")
    
    try:
        search_string=int(search_string)
    except:
        search_string=search_string   
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select *  from "SMB"."SMB - Base Price - Category Addition" where extract(month from "date_time")=extract(month from now()) order by "id" desc OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Base Price - Category Addition"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count,"date_time":date_time},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@app.route('/delete_record_baseprice',methods=['POST','GET','DELETE'])
def delete_record():  
    id_value=request.args.get('id')
    try:
        query='''delete  from "SMB"."SMB - Base Price - Category Addition" where "id"={}'''.format(id_value)
        db.insert(query)
        
        return {"status":"success"},200
    except:
        return {"status":"failure"},500


@app.route('/get_record_baseprice',methods=['GET','POST'])       
def get_record():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Base Price - Category Addition" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@app.route('/add_record_baseprice',methods=['POST'])
def add_record():
    
    today = date.today()
    username = getpass.getuser()
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    BusinessCode=(query_parameters['BusinessCode'])
    Market_Country =( query_parameters["Market_Country"])
    Product_Division =( query_parameters["Product_Division"])
    Product_Level_02 =( query_parameters["Product_Level_02"])
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    
  
   
    id_value=db.query('select max("id") from "SMB"."SMB - Base Price - Category Addition"')
    id_value=(id_value[0][0]+1)
   
    input_tuple=(id_value,username,date_time,BusinessCode,Market_Country,Product_Division,Product_Level_02,Document_Item_Currency,Amount,Currency.strip("'"))
    
    query='''INSERT INTO "SMB"."SMB - Base Price - Category Addition"(
         "id",
         "Username",
         "date_time",
         "BusinessCode",
         "Market - Country",
    	 "Product Division",
         "Product Level 02",
         "Document Item Currency",
         "Amount",
         "Currency")
         VALUES{};'''.format(input_tuple)
    print(query)
         
   
    db.insert(query)
    return {"status":"success"},200

   
@app.route('/Base_Price_Upload', methods=['GET','POST'])
def  SMB_upload():
    
    f=request.files['filename']
    try:
            
        f.save(input_directory+f.filename)
        smb_df=pd.read_excel(input_directory+f.filename)
        
        df=smb_df[['BusinessCode','Market - Country','Product Division','Product Level 02','Document Item Currency', 'Amount', 'Currency']]  
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table},200
    except:
        return {"statuscode":500,"message":"incorrect"},500


@app.route('/Base_Price_validate', methods=['GET','POST'])
def  SMB_validate():
    
        
        json_data=json.loads(request.data)
        
        try:
            df=pd.DataFrame(json_data["billet"])  
            
            username = getpass.getuser()
            now = datetime.now()
            date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
            
            id_value=db.query('select max("id") from "SMB"."SMB - Base Price - Category Addition"')
            id_value=(id_value[0][0]+1)
            
            df.columns = df.columns.str.replace('_', ' ')
            df.rename(columns={"Market Country":"Market - Country"},inplace=True)
            
            
            
            df.insert(0, 'id', range(id_value, id_value + len(df)))
            df.insert(1,'Username',username)
            df.insert(2,'date_time',date_time)
            
            df['id']=df['id'].astype(int)
            df['date_time']=pd.to_datetime(df['date_time'])
            
            df.to_sql("SMB - Base Price - Category Addition",con=engine, schema='SMB',if_exists='append', index=False)
            
            return {"status":"success"},200
        except:
            return {"status":"failure"},500
        
         
@app.route('/Base_price_download',methods=['GET'])
def SMB_baseprice_download():
   
        now = datetime.now()
        df = pd.read_sql('''select *  from "SMB"."SMB - Base Price - Category Addition" where extract(month from "date_time")=extract(month from now()) order by "id" desc''', con=con)
        df.drop(['Username','date_time','id'],axis=1,inplace=True)
        df.to_excel('C:/Users/Administrator/Downloads/'+now.strftime("%d-%m-%Y-%H-%M-%S") +'bse_price.xlsx',index=False)
        return {"status":"success"},200
   

if __name__ == '__main__':
    app.run()

