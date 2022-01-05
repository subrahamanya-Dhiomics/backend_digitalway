# -*- coding: utf-8 -*-
"""
Created on Mon Jan  3 07:42:56 2022

@author: Administrator
"""



# -*- coding: utf-8 -*-
"""
Created on Fri Dec 31 08:10:19 2021

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

app= Flask(__name__)

CORS(app)

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
 #           print('inside insert')
            var = 'failed'
            try:
                self.cursor = self.connection.cursor()
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
app=Flask(__name__)
CORS(app)

con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')
cursor=con.cursor()
@app.route('/existUsername',methods=['GET','POST'])
def valid_user():
    username = request.args.get('username')   
    try:       
        query_1='''select distinct(1) user_name from  user_management_ocp.user_details  where  user_name='{}' '''.format(username)  
        print(query_1)
        cursor.execute(query_1)
        status=cursor.fetchall()[0][0]
        return{"status":"Exist Username"}
    except:
        return{"status":"Not-Exist Username"}
    
@app.route('/existEmail',methods=['GET','POST'])
def valid_email():
    email = request.args.get('email')
    try:     
        query_1='''select distinct(1) email from  user_management_ocp.user_details  where  email='{}' '''.format(email)   
        print(query_1)
        cursor.execute(query_1)
        status=cursor.fetchall()[0][0] 
        return{"status":"Exist-Email"}
    except:
        return{"status":"Not-Exist Email"}
    
    
@app.route('/insert_values',methods=['POST','GET','PUT'])

def insert_values():
    Request_body = request.get_json()
    First_name=Request_body['first_name']
    Middle_name=Request_body['middle_name']
    Last_name=Request_body['last_name']
    User_name=Request_body['username']
    Email=Request_body['email']
    Phone_number=Request_body['phone_no']
    Address = Request_body['address']
    Group_id = Request_body['user_group']
    
    save_with_table=(User_name,First_name,Middle_name,Last_name,Address,Email,Phone_number,Group_id)

    try:
           query='''insert into  user_management_ocp.user_details (
           "user_name",
           "first_name",
           "middle_name",
           "last_name",
           "address",
           "email",
           "phone_number",
           "group_id"
           )  VALUES {}'''.format(save_with_table)
           db.insert(query)
           print(query)
           return {"status":"success",'status_code':204}

        
    except:
        return {"status":"failure",'status_code':500}

@app.route('/user_access', methods=['POST','GET'])
def useraccess():
    query='''SELECT  groupid, group_description FROM user_management_ocp.group'''
    df=pd.read_sql(query,con=con)
    data=json.loads(df.to_json(orient='records'))
    return {"data":data}
    
if __name__=="__main__":
    app.run()