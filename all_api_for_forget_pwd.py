
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
import smtplib
import re
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

@app.route('/valid_email',methods=['POST','GET'])
def valid_email():
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    email = request.args.get('email')
    if(re.fullmatch(pattern, email)):
        query_1='''select distinct(1) email from  user_management_ocp.user_details  where  email='{}' '''.format(email)   
        status=db.query(query_1)[0][0]        
        return{"status":"Exist-Email"},204
    else:
        return{"status":"Not-Exist Email"},500
            
@app.route('/password',methods=['POST','GET'])
def password():
    password=request.args.get('password')
    try:
        query_1='''select distinct(1) email from  user_management_ocp.user_details  where  password='{}' '''.format(password)   
        result=db.query(query_1)[0][0]        
        if result=='failed' :raise ValueError
        return{"status":"password-correct"},204
    except:
        return{"status":"password-incorrect"},500
        
def send_email(receivers):
    sender = 'ymsyathish@gmail.com'
#    receivers = ['yathish.susandhi@gmail.com']

    message = """From: No Reply <no_reply@mydomain.com>
    To: Person <yathish.susandhi@gmail.com>
    Subject: reset passoword

    This is a test e-mail message.
    Link:www.google.com
    """
    try :

        server = smtplib.SMTP('smtp.gmail.com:587')
        server.ehlo()
        server.starttls()
        server.login(sender, 'offertool2w2016.')
        server.sendmail(sender, receivers, message)
        server.close()
        
        return 1
        
    except :
       return 0
@app.route('/reset_password',methods=['POST','GET'])
def reset_password():
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    email = request.args.get('email')
    receivers=email
    try :
        if(re.fullmatch(pattern, email)):
            query_1='''select distinct(1) email from  user_management_ocp.user_details  where  email='{}' '''.format(email)   
            status=db.query(query_1)[0][0] 
            if(status==0):
 #               receivers=[]
                email_sent_status = send_email(receivers)
                if email_sent_status :
                    return{"status":"Email sent successfully"},204
                else :
                    return{"status":"Error sending email to the user, Please try again later"},500
            else:
                return{"status":"Email does not exist"},500
        else :
            return{"status":"Please send the valid email address"},500
    except :
        return{"status":"Not-Exist Email"},500
    

if __name__ == '__main__':
   app.run()

