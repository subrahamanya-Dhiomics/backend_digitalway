# -*- coding: utf-8 -*-
"""
Created on Thu Dec 23 17:39:09 2021

@author: subbu
"""
from flask import Blueprint
import pandas as pd

import json
from flask import Flask, request, send_file, render_template, make_response
from flask import jsonify
from flask_cors import CORS
from smb_phase1 import Database

import psycopg2

from sqlalchemy import create_engine
from smb_phase1 import con
from smb_phase2 import token_required

engine = create_engine('postgresql://postgres:ocpphase01@ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com:5432/offertool')

# class Database:
#     host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com'  # your host
#     user='postgres'      # usernames
#     password='ocpphase01'
    
#     db='offertool'
   
#     def __init__(self):
#             print('Connection Opened')
#             self.connection = psycopg2.connect(dbname=self.db,user=self.user,password=self.password,host=self.host)
           
   
#     def insert(self, query):
#             print('inside insert')
#             var = ''
#             try:
#                 self.cursor = self.connection.cursor()
# #                print("HEY")
#                 var = self.cursor.execute(query)
#                 print(str(var))
#                 self.connection.commit()
#             except:
#                 self.connection.rollback()
#             finally:
#                 self.cursor.close()
#                 print('Cursor closed')
   
#             return(var)

#     def query(self, query):
#         try:
#             self.cursor = self.connection.cursor()
#             print('inside query')
#             self.cursor.execute(query)
#             return self.cursor.fetchall()
#         finally:
#             self.cursor.close()
#             print('Cursor closed')

db=Database()

user_management_app = Blueprint('user_management_app', __name__)

CORS(user_management_app)

# con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')



cursor=con.cursor()

 
@user_management_app.route("/group_management_data",methods=['GET','POST'])
@token_required

def data_menus():
    
    sub_menu_items=''
    query="select concat(left(max(groupid),1)::varchar,right(max(groupid),1)::int+1 ) from user_management_ocp.group"
    print(query)
    group_id=db.query(query)[0][0]
                                                                                                
    menu_items=["Management","Alloy_Scrap","SMB","SMB_Minibar"]
   
   
    return {"menu_items":menu_items,"sub_menu_items":sub_menu_items,"group_id":group_id}








@user_management_app.route("/group_management_insert",methods=['GET','POST'])
@token_required
def data_menu(): 
    
    data=json.loads(request.data)
    group_code=data['group_code']
    group_description=data["group_description"]
   
    menu_items=data['menu']
    
    query=''' insert into user_management_ocp.group(groupid,group_description) values('{}','{}') '''.format(group_code,group_description)
    query1=''' insert into user_management_ocp.group_access values('{}','{}')  '''.format(group_code,str(set(menu_items)).replace("'",'"'))
    print(query)
    print(query1)
    db.insert(query)
    db.insert(query1)
    
    
    return {"status":"success"},200
   
    
    

@user_management_app.route('/existUsername',methods=['GET','POST'])
@token_required
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
    
@user_management_app.route('/existEmail',methods=['GET','POST'])
@token_required
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
    
    
@user_management_app.route('/insert_values',methods=['POST','GET','PUT'])
@token_required

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

@user_management_app.route('/user_access', methods=['POST','GET'])
@token_required
def useraccess():
    query='''SELECT  groupid, group_description FROM user_management_ocp.group'''
    df=pd.read_sql(query,con=con)
    data=json.loads(df.to_json(orient='records'))
    return {"data":data},200



