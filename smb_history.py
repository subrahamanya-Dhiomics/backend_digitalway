# -*- coding: utf-8 -*-
"""
Created on Wed Nov  3 10:02:03 2021

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

from smb_phase1 import con


from smb_phase1 import Database

engine = create_engine('postgresql://postgres:ocpphase01@ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com:5432/offertool')
     

smb_history = Blueprint('smb_history', __name__)

CORS(smb_history)

db=Database()

input_directory="C:/Users/Administrator/Documents/SMB_INPUT/"


#download_path='C:/SMB/smb_download/'



@smb_history.route('/history_delivering_mill_MiniBar',methods=['GET'])
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
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       

    

@smb_history.route('/history_delivering_mill',methods=['GET'])
def  download_delivery_mill_history():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Delivery Mill_History"   OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Delivery Mill_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500



@smb_history.route('/history_Base_Price',methods=['GET'])
def download_base_price_history():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Base Price - Category Addition_History"   OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Base Price - Category Addition_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       



@smb_history.route('/history_Base_Price_minibar',methods=['GET'])
def download_base_price_minibar_history():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Base Price - Category Addition - MiniBar_History"   OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Base Price - Category Addition - MiniBar_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Customer":"Market_Customer"},inplace=True)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       
   



@smb_history.route('/history_Base_Price_Incoterm_Exceptions',methods=['GET'])
def download_base_price_Incoterm_Exceptions_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Base Price - Incoterm Exceptions_History"   OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Base Price - Incoterm Exceptions_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        # print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       
 
    


@smb_history.route('/history_Extra_Certificate',methods=['GET'])
def download_Extra_Certificate_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Certificate_History"   OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Certificate_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500   
    
@smb_history.route('/history_Extra_Certificate_miniBar',methods=['GET'])
def download_Extra_Certificate_miniBar_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Certificate - MiniBar_History"   OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Certificate - MiniBar_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Customer":"Market_Customer","Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        # print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       





@smb_history.route('/history_Extra_Freight_Parity',methods=['GET'])
def download_Extra_Freight_Parity_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Freight Parity_History"   OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Freight Parity_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        # print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       





@smb_history.route('/history_Extra_Freight_Parity_MiniBar',methods=['GET'])
def download_Extra_Freight_Parity_MiniBar_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Freight Parity - MiniBar_History"   OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Freight Parity - MiniBar_History"')[0][0]
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
    except:
        return {"statuscode":500,"msg":"failure"},500       
    
    





@smb_history.route('/history_Extra_Grade',methods=['GET'])
def download_Extra_Grade_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Grade_History"   OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Grade_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        # print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       



@smb_history.route('/history_Extra_Grade_MiniBar',methods=['GET'])
def download_Extra_Grade_MiniBar_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Grade - MiniBar_History"  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Grade - MiniBar_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Customer":"Market_Customer","Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        # print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       




@smb_history.route('/history_Extra_Length_Logistic',methods=['GET'])
def download_Extra_Length_Logistic_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Length Logistic_History"  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Length Logistic_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        # print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       




@smb_history.route('/history_Extra_Length_Logistic_MiniBar',methods=['GET'])
def download_Extra_Length_Logistic_MiniBar_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Length Logistic - MiniBar_History"  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Length Logistic - MiniBar_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Customer":"Market_Customer","Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        # print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       





@smb_history.route('/history_Extra_Length_Production',methods=['GET'])
def download_Extra_Length_Production_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Length Production_History"  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Length Production_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        # print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       





@smb_history.route('/history_Extra_Length_Production_MiniBar',methods=['GET'])
def download_Extra_Length_Production_MiniBar_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Length Production - MiniBar_History" OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Length Production - MiniBar_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Customer":"Market_Customer","Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        # print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       
 
    




@smb_history.route('/history_Extra_Profile',methods=['GET'])
def download_Extra_Profile_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Profile_History" OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Profile_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        # print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       
    
    




@smb_history.route('/history_Extra_Profile_MiniBar',methods=['GET'])
def download_Extra_Profile_MiniBar_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Profile - MiniBar_History" OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Profile - MiniBar_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Customer":"Market_Customer","Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        # print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       

    




@smb_history.route('/history_Extra_Profile_Iberia_And_Italy',methods=['GET'])
def download_Extra_Profile_Iberia_And_Italy_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Profile Iberia and Italy_History" OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Profile Iberia and Italy_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        # print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       
     
    




@smb_history.route('/history_Extra_Profile_Iberia_And_Italy_MiniBar',methods=['GET'])
def download_Extra_Profile_Iberia_And_Italy_MiniBar_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Profile Iberia and Italy - MiniBar_History" OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Profile Iberia and Italy - MiniBar_History"')[0][0]
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
    except:
        return {"statuscode":500,"msg":"failure"},500       
       
    




@smb_history.route('/history_Extra_Transport_Mode',methods=['GET'])
def download_Extra_Transport_Mode_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Transport Mode_History" OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Transport Mode_History"')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
       
        # print(df['updated_on'])
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500       





@smb_history.route('/history_Extra_Transport_Mode_MiniBar',methods=['GET'])
def download_Extra_Transport_Mode_MiniBar_History():
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
 
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Transport Mode - MiniBar_History" OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Transport Mode - MiniBar_History"')[0][0]
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
    except:
        return {"statuscode":500,"msg":"failure"},500       
     
    




