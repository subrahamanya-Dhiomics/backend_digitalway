# -*- coding: utf-8 -*-
"""
Created on Thu Dec 23 10:49:02 2021

@author: Mahesh
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import json
import numpy as np
import pandas as pd
import traceback
import time
import json
from json import JSONEncoder
from collections import OrderedDict
from flask import Blueprint
import calendar
import psycopg2
import shutil
from pathlib import Path
import os
from sqlalchemy import create_engine
import getpass
from datetime import datetime
from datetime import date
import random


con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')
cur = con.cursor()
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
app = Flask(__name__)
CORS(app)

@app.route('/user_access', methods=['POST','GET'])
def useraccess():
    query='SELECT  distinct(group_description) as group FROM user_management_ocp.group'''
    df=pd.read_sql(query,con=con)
    group=list(df['group'])
    return {"group_description":group}
    
@app.route('/emailexist',methods=['POST','GET'])
def usernameAndEmailExist():
    username=request.args.get('username')
    email=request.args.get('email')
    try:       
        query = '''select distinct(1)  from  user_management_ocp.user_details  where user_name='{}' and email='{}' '''.format(username,email)     
        #data=db.query(query)
        cur.execute(query)
        print(query)
        status=cur.fetchall()[0][0]
    except:
        status=0
    if status==1:
        return{"status":"exist-account"},200
    else:
        return{"status":"not-exist"},500    
if __name__ == "__main__":
    app.run()