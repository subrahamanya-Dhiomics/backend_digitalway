
"""
Created on Wed Nov 10 07:21:49 2021

@author: Administrator
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
            
db=Database()
               
class data:
    
    tablenames=json.loads(pd.DataFrame(db.query('select tablename from "SMB".table_mapping'),columns=['tablename']).to_json(orient='records'))
    
    columns=['BusinessCode',
 'Customer_Group',
 'Market_Customer',
 'Market_Country',
 'Beam_Category',
 'Document_Item_Currency',
 'Amount',
 'Currency',
 'Product_Division',
 'Product_Level_02',
 'Incoterm1',
 'Delivering_Mill',
 'Certificate',
 'Grade_Category',
 'Market_Customer_Group',
 'Zip_Code_(Dest)',
 'Country_Group',
 'Length',
 'Length_From',
 'Length_To',
 'Transport_Mode',
 'Product_Level_04',
 'Product_Level_05',
 'sequence_id' ,
'table_name',
'id'
 ]
  

data=data()



input_directory="C:/Users/Administrator/Documents/SMB_INPUT/"

#download_path='C:/SMB/smb_download/'

con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')

app=Flask(__name__)
CORS(app)



table_column_mapping={'SMB - Base Price - Category Addition - MiniBar': ['BusinessCode',
  'Customer Group',
  'Market - Customer',
  'Market - Country',
  'Beam Category',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Base Price - Category Addition': ['BusinessCode',
  'Market - Country',
  'Product Division',
  'Product Level 02',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Base Price - Incoterm Exceptions': ['Market - Country',
  'Customer Group',
  'Incoterm1',
  'Product Division',
  'Beam Category',
  'Delivering Mill',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Certificate - MiniBar': ['BusinessCode',
  'Customer Group',
  'Market - Customer',
  'Market - Country',
  'Certificate',
  'Grade Category',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Certificate': ['BusinessCode',
  'Certificate',
  'Grade Category',
  'Market - Country',
  'Delivering Mill',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Delivery Mill - MiniBar': ['Market - Country',
  'Market - Customer Group',
  'Market - Customer',
  'Delivering Mill',
  'Product Division',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Delivery Mill': ['BusinessCode',
  'Market - Country',
  'Delivering Mill',
  'Product Division',
  'Beam Category',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Freight Parity - MiniBar': ['Delivering Mill',
  'Market - Country',
  'Market - Customer Group',
  'Market - Customer',
  'Zip Code (Dest)',
  'Product Division',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Freight Parity': ['Delivering Mill',
  'Market - Country',
  'Zip Code (Dest)',
  'Product Division',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Grade - MiniBar': ['BusinessCode',
  'Customer Group',
  'Market - Customer',
  'Market - Country',
  'Grade Category',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Grade': ['BusinessCode',
  'Grade Category',
  'Country Group',
  'Market - Country',
  'Product Division',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Length Logistic - MiniBar': ['Customer Group',
  'Market - Customer',
  'Market - Country',
  'Delivering Mill',
  'Length',
  'Length From',
  'Length To',
  'Transport Mode',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Length Logistic': ['Country Group',
  'Market - Country',
  'Delivering Mill',
  'Length',
  'Length From',
  'Length To',
  'Transport Mode',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Length Production - MiniBar': ['BusinessCode',
  'Customer Group',
  'Market - Customer',
  'Market - Country',
  'Delivering Mill',
  'Length',
  'Length From',
  'Length To',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Length Production': ['BusinessCode',
  'Country Group',
  'Market - Country',
  'Delivering Mill',
  'Length',
  'Length From',
  'Length To',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Profile - MiniBar': ['BusinessCode',
  'Customer Group',
  'Market - Customer',
  'Market - Country',
  'Product Level 04',
  'Product Level 05',
  'Product Level 02',
  'Delivering Mill',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Profile Iberia and Italy - MiniBar': ['BusinessCode',
  'Market - Country',
  'Market - Customer Group',
  'Market - Customer',
  'Delivering Mill',
  'Product Level 02',
  'Product Level 05',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Profile Iberia and Italy': ['BusinessCode',
  'Market - Country',
  'Delivering Mill',
  'Product Level 02',
  'Product Level 05',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Profile': ['BusinessCode',
  'Market - Country',
  'Product Division',
  'Product Level 04',
  'Product Level 05',
  'Product Level 02',
  'Delivering Mill',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Transport Mode - MiniBar': ['Product Division',
  'Market - Country',
  'Market - Customer Group',
  'Market - Customer',
  'Transport Mode',
  'Document Item Currency',
  'Amount',
  'Currency'],
 'SMB - Extra - Transport Mode': ['Product Division',
  'Market - Country',
  'Transport Mode',
  'Document Item Currency',
  'Amount',
  'Currency']}



@app.route("/universal_add_smb")
def universal_add_multiple():
   
    
    columns=data.columns
    json=[{}]
    
    for i in columns:
        json[0][i]=request.args.get(i)
    df=pd.DataFrame(json)
    df['flag']='add'
    
    df.to_sql("SMB_Aproval", con=engine, schema="SMB", if_exists='append')
    
    
    return "success"
    

@app.route("/universal_update_smb",methods=['GET','POST'])
def universal_update_smb():
    
    
    columns=data.columns
   
    json=[{}]
    
    for i in columns:
        json[0][i]=request.args.get(i)
        
         
    print(json)
    df=pd.DataFrame(json)
    df['Username']='subbu'
    df['aprover']='jaidev'
    
    df.to_sql("SMB_Aproval", con=engine, schema="SMB", if_exists='append')
    
    
    return "success"

@app.route("/universal_update_multiple",methods=['GET','POST'])
def universal_aprove_smb():
    
    table_name=request.args.get('table_name')
   
    
    
    json_data=json.loads(request.data)
    df=pd.DataFrame(json_data["billet"]) 
    
    df['Username']='subbu'
    df['aprover']='jaidev'
   
    
    
    
    df.to_sql("SMB_Aproval", con=engine, schema="SMB", if_exists='append')
    return "success"



@app.route("/universal_aprove_data",methods=['GET','POST'])
def universal_aprove_data():
    table_name=request.args.get('table_name')
    
    
    
    
    
    df=pd.read_sql('''select * from  "SMB"."SMB_Aproval" where table_name='{}' '''.format(table_name))
    
    table=json.loads(df.to_json(orient='records'))
    
    return {"data":table}


@app.roue("/universal_aprove",methods=['GET','POST'])
def universal_aprove():
    
    table_name=request.args.get('table_name')
    history_table=table_name+"_History"
    
    data=json.loads(request.data)
    json_data=json.loads(request.data)
    df=pd.DataFrame(json_data)
    
    add_df=df[df['flag']=='add']
    update_df=df[df['flag']=='update']
    id_tuple=tuple(update_df['id'])
    
    column_str=''
    for i in table_column_mapping[table_name]:
        column_str+=","
        column_str+= '"'+i+'"'
    
    
    query1='''INSERT INTO "SMB"."{}" SELECT "id","Username",now(){} FROM "SMB"."{}" WHERE "id" in {} '''.format(history_table,column_str,table_name,id_tuple)

    
    query2='''UPDATE "SMB".'''+ table_name+'''
     SET 
    "Username"='%s',
    "BusinessCode"='%s', "Customer Group"='%s',"Market - Customer"='%s', "Market - Country"='%s', "Beam Category"='%s',"Document Item Currency"='%s', "Amount"='%s', "Currency"=''%s'',
    "updated_on"='%s'
     WHERE "id"= '%s' ''' % tuple(add_df.loc[i])
     
     
        
if __name__ == '__main__':
    app.run()
