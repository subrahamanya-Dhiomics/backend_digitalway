# -*- coding: utf-8 -*-
"""
Created on Wed Dec 29 11:45:26 2021

@author: Administrator
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Dec 23 11:17:05 2021

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

@app.route('/insert_values',methods=['POST','GET','PUT'])

def insert_values():
    Request_body = request.get_json()
    First_name=Request_body['First_name']
    Middle_name=Request_body['Middle_name']
    Last_name=Request_body['Last_name']
    User_name=Request_body['User_name']
    Email=Request_body['Email']
    Phone_number=Request_body['Phone_number']
    Address=Request_body['Address']
    User_id=Request_body['user_id']
    
    
    save_with_table=(User_name,First_name,Middle_name,Last_name,Address,Email,Phone_number)

    try:
           query_a='''insert into user_management_ocp.user_details (user_name,first_name,middle_name,last_name,address,email,phone_number)  VALUES{}'''.format(save_with_table)
           result_1=db.insert(query_a)
           if result_1=='failed' :raise ValueError
   
           data_tuple=(User_name,First_name,Middle_name,Last_name,Address,Email,Phone_number,User_id)
           query_b=''' Update user_management_ocp.user_details set
           user_name='%s',first_name='%s',middle_name='%s',last_name='%s',address='%s',
           email='%s',phone_number='%s' where user_id='%s' '''%data_tuple
           result_2=db.insert(query_b)
#          print(query_b)
           if result_2=='failed' :raise ValueError

           return {"status":"success",'status_code':204}

        
    except:
        return {"status":"failure",'status_code':500}

@app.route('/valid_user',methods=['GET''POST'])

def valid_user():
    Request_body = request.get_json()
    Username=Request_body['Username']
    Email=Request_body['Email']   
    
    
    try:
        
        query_1='''select distinct(1) username,email from  user_management_ocp.user_details  where user_name='{}' or email='{}' '''.format(Username,Email)
     
        print(query_1)
        cursor.execute(query_1)
        status=cursor.fetchall()[0][0]
     
    except:
        status=0
        
    
    if status==1:
        return{"status":"exist-account",'status_code':204}
    else:
        return{"status":"not-exist",'status_code':500}
    
@app.route('/Forget_password',methods=['GET''POST'])

def Forget_password():
    Request_body = request.get_json()
    Registered_email=Request_body['registered_email']
    
    try:
        
        query_3='''select distinct(1) email from  user_management_ocp.user_details  where email='{}' '''.format(Email)
     
        print(query_1)
        cursor.execute(query_3)
        status=cursor.fetchall()[0][0]
     
    except:
        status=0
        
    
    if status==1:
        
        link =" www.google.com"
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com",465, context=context) as server:
         .  login("yathish.susandhi@gmail.com","mandrimandri")
            server.sendmail("yathish.susandhi@gmail.com", "ymsyathish@gmail.com", link)
  
    
    else:
        return{"status":"not-exist",'status_code':500}    
    
        
         



    
    
@app.route('/Reset_pasword',methods=['GET''POST'])
def Reset_password():
    
         
if __name__=="__main__":
    app.run()

'''# -*- coding: utf-8 -*-
"""
Created on Thu Dec 30 11:28:02 2021

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

@app.route('/insert_values',methods=['POST','GET','PUT'])

def insert_values():
    Request_body = request.get_json()
    First_name=Request_body['First_name']
    Middle_name=Request_body['Middle_name']
    Last_name=Request_body['Last_name']
    User_name=Request_body['User_name']
    Email=Request_body['Email']
    Phone_number=Request_body['Phone_number']
    Address=Request_body['Address']
#    User_id=Request_body['user_id']
    
    
    save_with_table=(User_name,First_name,Middle_name,Last_name,Address,Email,Phone_number)

    try:
           query_a='''insert into user_management_ocp.user_details (user_name,first_name,middle_name,last_name,address,email,phone_number)  VALUES{}'''.format(save_with_table)
           result_1=db.insert(query_a)
           if result_1=='failed' :raise ValueError
   
           data_tuple=(User_name,First_name,Middle_name,Last_name,Address,Email,Phone_number,User_id)
           query_b=''' Update user_management_ocp.user_details set
           user_name='%s',first_name='%s',middle_name='%s',last_name='%s',address='%s',
           email='%s',phone_number='%s' where user_id='%s' '''%data_tuple
           result_2=db.insert(query_b)
#          print(query_b)
           if result_2=='failed' :raise ValueError

           return {"status":"success",'status_code':204}

        
    except:
        return {"status":"failure",'status_code':500}

@app.route('/valid_user',methods=['GET''POST'])

def valid_user():
    Request_body = request.get_json()
    Username=Request_body['username']
    Email=Request_body['email']   
    
    
    try:
        
        query_1='''select distinct(1) username,email from  user_management_ocp.user_details  where user_name='{}' or email='{}' '''.format(Username,Email)
     
        print(query_1)
        cursor.execute(query_1)
        status=cursor.fetchall()[0][0]
     
    except:
        status=0
        
    
    if status==1:
        return{"status":"exist-account",'status_code':204}
    else:
        return{"status":"not-exist",'status_code':500}
    
if __name__=="__main__":
    app.run()

'''