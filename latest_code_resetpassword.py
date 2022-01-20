# -*- coding: utf-8 -*-
"""
Created on Wed Jan 19 13:32:53 2022

@author: Administrator
"""

import base64
import pandas as pd
import jwt
from cryptography.fernet import Fernet
import json
from flask import Flask, request, send_file, render_template, make_response
from flask import jsonify, Blueprint, current_app
from flask_cors import CORS
from json import JSONEncoder
from collections import OrderedDict
import psycopg2
from sqlalchemy import create_engine
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import re
from flask import Flask
app= Flask(__name__)
reset_password = Blueprint('reset_password', __name__)
CORS(reset_password)
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
reset_password = Blueprint('reset_password', __name__)
CORS(reset_password)

con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')
cursor=con.cursor()
def send_email(receiver,fname,mname,lname,encoded_user_id):
    me='''ymsyathish@gmail.com''' #ranjitkumar@digitalway-lu.com   ymsyathish@gmail.com
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "Reset Password"
    msg['From'] = me
    msg['To'] = receiver
    #print(user)
    html='''<!DOCTYPE html>
<html lang="en">
<head>
  <title>Bootstrap Example</title>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css">
  <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script>
  <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
</head>
<body>
     
<style>
    th{
        font-family: 'Times New Roman', serif;
        font-size: 15px;
    }
    td{
        font-family:'Arial';
        font-size: 13px;

    }
    img {
        max-width: 80px ;
        max-height: 60px ;
    }
</style> 

<div style="text-align:center">
    <img src="https://drive.google.com/thumbnail?id=1nRZ2KomzjstN68nJL5nMnDv3HIcj2-Mt" alt="logo" />
</div>
<table>
    <tr>
        <th>Hello&nbsp;'''+fname+'''&nbsp;'''+mname+'''&nbsp;'''+lname+''',</th>
    </tr>
    <tr>
        <td></td>
        <td>
            We have sent you this email in response to your request to reset your password. 
            You can use the following link to reset your Password.
        </td>
    </tr>
    <tr>
        <td></td>
        <td>
            <div style="text-align: center;">
                <a href="''' + current_app.config["environment_url"] + '''={}"><u> Reset Password <u></a>
                <i class="fa fa-lock icon" style="font-size:20px;margin:10px;"></i>
            </div>
        </td>
    </tr>
    <tr>
        <td></td>
        <td>
            If you didn't request a password reset,you can ignore this email. We recommend that you keep your
            password secure and not share with anyone.
        </td>
    </tr>
</table>

</body>
</html>'''.format(encoded_user_id)
    
    
    part = MIMEText(html, 'html')
    msg.attach(part)
    server = smtplib.SMTP('smtp.gmail.com',587)
    server.ehlo()
    server.starttls()
    server.login(me, 'offertool2w2016.')
    server.sendmail(me, receiver, msg.as_string())     
    server.close()
    
    
    return{'status':"Success"},200

#key for userid encode&decode
key = Fernet.generate_key()
fernet = Fernet(key)

#key for password store
@reset_password.route('/resetemail',methods=['POST','GET'])
def resetemail():
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    email = request.args.get('email')
    
    try:
        if(re.fullmatch(pattern, email)):
                query_1='''select distinct(1) email,first_name,middle_name,last_name,user_id from  user_management_ocp.user_details  where  email='{}' '''.format(email)   
                status=db.query(query_1)
                user_id=status[0][4]
                print(user_id)
                user_id_str=str(user_id)
                encoded_user_id = fernet.encrypt(user_id_str.encode()).decode()
                print(encoded_user_id)
                #user=status[0][1]+''+status[0][2]+''+status[0][3]
                fname=status[0][1]
                mname=status[0][2]
                lname=status[0][3]
                
                status=status[0][0]
                
                receivers=email
                if(status==1):
                    
                    email_sent_status = send_email(receivers,fname,mname,lname,encoded_user_id)
                    if email_sent_status:
                        return{"status":"Email sent successfully"},200
                    else:
                        return{"status":"Error sending email to the user, Please try again later"},404
                else:
                    return{"status":"Email does not exist"},404
        else :
                return{"status":"Please Enter Valid Email"},404        
    except :
        return{"status":"Failure"},404


@reset_password.route('/reset',methods=['POST','GET'])
def resetPassword():
    new_password=request.args.get('new_password')
    
    new_password_str=str(new_password)
    encrypte_new_password = base64.b64encode(new_password_str.encode("utf-8"))
    encrypte_new_password=encrypte_new_password.decode()
    print("newpws",encrypte_new_password)
    
    user_id_encrypt=request.args.get('encrypt_user_id')
    print(user_id_encrypt)
    
    try:    
         user_id = fernet.decrypt(user_id_encrypt.encode()).decode()
         query_1='''select distinct(1)  from  user_management_ocp.user_details  where  user_id='{}' '''.format(user_id)
         print(query_1)
         status=db.query(query_1)
         status=status[0][0]
    except:
         return{"status":"Failure","status_code":500}
    if(status==1):
            query='''update user_management_ocp.user_details set "password"='{}' where "user_id"={}'''.format(encrypte_new_password,user_id)
            print()
            status=db.insert(query)
            return {"status":"successfully_updated","status_code":200} 
    else:
        
        return{"status":"invalid_userid" ,"status_code":500}
   
@reset_password.route('/login', methods=['POST','GET'])
def login():
    username=request.args.get('username')
    password=request.args.get('password')
    
    query="select key_value  from  user_management_ocp.user_details  where user_name='{}'".format(username)
    
    cursor.execute(query)
    key=cursor.fetchall()[0][0]
    
    query="select password  from  user_management_ocp.user_details  where user_name='{}'".format(username)
    
    cursor.execute(query)
    dbpassword=cursor.fetchall()[0][0]
    
    fernet = Fernet(key.encode())
    
    decrypted_password = fernet.decrypt(dbpassword.encode()).decode()
    #print(decrypted_password, password)
    
    """
    password_str=str(password)
    encrypted_password=fernet.encrypt(password_str.encode()).decode()

        
    query="select distinct(1) from  user_management_ocp.user_details  where user_name='{}' and password='{}'".format(username,encrypted_password)

     
    cursor.execute(query)
    status=cursor.fetchall()[0][0]
     
#    except:
#        status=0
        
    """
    if decrypted_password == password:
        df=pd.read_sql("select * from  user_management_ocp.user_details  where user_name='{}'".format(username),con=con)
        user=json.loads(df.to_json(orient='records'))[0]
       
        token = jwt.encode({ 'user':username }, current_app.config["LOGIN_SECRETKEY"])
        
        return jsonify({'token': token,'user':user}),200
    else:
        return make_response('Unable to verify',  {'WWW-Authenticate': 'Basic realm: "Authentication Failed "'}),500
    
'''
@app.route('/login', methods=['POST','GET'])
def login():
    username=request.args.get('username')
    password=request.args.get('password')
    print(username,password)
    
    password=str(password)
    encrypte_password = base64.b64encode(password.encode("utf-8"))
    encrypte_password=encrypte_password.decode()
    print('encrypte_pass',encrypte_password)
    #password='b'+"'{}'".format(status)
    
    
    try:
     query="select distinct(1) from  user_management_ocp.user_details  where user_name='{}' and password='{}'".format(username,encrypte_password)
     status = db.query(query)[0][0]
    except:
        status=0
    if status==1:
        df=pd.read_sql("select * from  user_management_ocp.user_details  where user_name='{}'".format(username),con=con)
        user=json.loads(df.to_json(orient='records'))[0]
        
       
        token = jwt.encode({
            'user':username
        },
            app.config['SECRET_KEY'])
        
        return jsonify({'token': token,'user':user}),200
    else:
        return make_response('Unable to verify',  {'WWW-Authenticate': 'Basic realm: "Authentication Failed "'}),500
'''

