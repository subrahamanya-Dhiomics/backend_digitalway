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

import psycopg2

from sqlalchemy import create_engine

from smb_phase2 import token_required

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

db=Database()

user_management_app = Blueprint('user_management_app', __name__)

CORS(user_management_app)

con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')



cursor=con.cursor()

 
@user_management_app.route("/group_management_data",methods=['GET','POST'])
@token_required

def data_menus():
    
    
    try:
        data=json.loads(request.data)
        menu_id=data['menu_id']
        
    except:
        menu_id=['df']
        
    
    print(menu_id)
    
    menu_id.append('all')
    menu_id=tuple(menu_id)
       
   
    
    query="select concat(left(max(groupid),1)::varchar,right(max(groupid),1)::int+1 ) from user_management_ocp.group"
    menu_items="select menuid,menudescription from user_management_ocp.menu_item_details "
    
    if(menu_id=='All' or menu_id==None or menu_id=='all'):
         sub_menu_items='''select "SubMenuID","SubMenuDescription" from user_management_ocp.submenu_item_details'''
    
       
    else:
        sub_menu_items=''' select menuid,"SubMenuID","SubMenuDescription" from user_management_ocp.menu_item_details inner join user_management_ocp.submenu_item_details on mid=mid  where  MenuID in {} '''.format(menu_id)

        
    print(sub_menu_items)
    menu_df=pd.read_sql(menu_items,con=con)
    sub_menu_df=pd.read_sql(sub_menu_items,con=con)
    group_code=db.query(query)[0][0]
    
    group_id=db.query(query)[0][0]
                                                                                                
    menu_items=json.loads(menu_df.to_json(orient='records'))
    sub_menu_items=json.loads(sub_menu_df.to_json(orient='records'))
   
   
    return {"menu_items":menu_items,"sub_menu_items":sub_menu_items,"group_id":group_id}








@user_management_app.route("/group_management_insert",methods=['GET','POST'])
@token_required
def data_menu(): 
    
    data=json.loads(request.data)
    group_code=data["group_code"]
    group_description=data["group_description"]
   
    df=pd.DataFrame(data["sub_menu"])
    
    
    group_tuple=(group_code,group_description)
    
    
   
    query="INSERT INTO user_management_ocp.group (groupid, group_description) VALUES {} ".format(group_tuple)
    print(query)
    db.insert(query)
    
                        
    
    df.insert(0,"groupid",group_code)
    df.drop(['SubMenuDescription'], axis = 1,inplace=True) 
   
    df.to_sql("group_access_details",con=engine, schema='user_management_ocp',if_exists='append', index=False)
    

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



