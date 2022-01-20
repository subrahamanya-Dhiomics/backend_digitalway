# -*- coding: utf-8 -*-
"""
Created on Thu Jan  6 13:16:41 2022

@author: Administrator
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 10:02:00 2022

@author: Administrator
"""

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


import pandas as pd
import time
import json
from flask import Flask, request, send_file, render_template, make_response
from flask import jsonify
from flask_cors import CORS
from json import JSONEncoder
from collections import OrderedDict
import psycopg2
import shutil
from pathlib import Path
import os
from sqlalchemy import create_engine
import getpass
from datetime import datetime,date
import smtplib
import re
from flask import Flask
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
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

def send_email(receiver,user):
    me='''ranjitkumar@digitalway-lu.com'''
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "Reset Password"
    msg['From'] = me
    msg['To'] = receiver
    print(user)
    html='''<html>
<title>Email Template</title>

<head>
    <!-- Add icon library -->
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css">
    <style>
        .container {
            font-family: Times New Roman;
            background-color: white;
            text-align: center;
            height: 98%;
        }

        /* Set a style for the reset button */
        .resetButton {
            background-color: #f47d30;
            color: black;
            padding: 8px 10px;
            margin: 8px 0px;
            border: none;
            cursor: pointer;
            width: auto;
            opacity: 0.9;

        }

        .resetButton:hover {
            opacity: 5;

        }

        p {
            font-size: 14px;
        }

        img {
            height: 80px;
            width: 145px;

        }
    </style>
</head>

<body>
    <form>
        <div class="container">
            <div style="text-align: center;">

                <img src="https://drive.google.com/thumbnail?id=1nRZ2KomzjstN68nJL5nMnDv3HIcj2-Mt" alt="logo" />
            </div>
            <h3 style="text-align: left; font-size: 18px;">Hello '''+ user+''',</h3>
            <div>

                <div style="text-align: justify; margin-left: 12%;">
                    <p>We heard that you lost your Password.Sorry about that! But don't worry! We have sent you this
                        email in response to your request to reset your password. You can use the following button
                        to reset your Password.
                    </p>
                </div>
                <div style="text-align: center;">
                    <i class="fa fa-lock icon" style="font-size:35px;"></i><br>
                    <button class="resetButton"><a href="https://smbprice.dcc-am.com/auth/reset-password">Reset Password</a></button>                
                </div>
                <div style="text-align: justify; margin-left: 12%;">
                    <p>If you didn't request a password reset,you can ignore this email. We recommend that you keep your
                        password secure and not share with anyone.
                    </p>
                </div>

            </div>
        </div>

    </form>

</body>

</html>'''
    
    
    part = MIMEText(html, 'html')
    msg.attach(part)
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo()
    server.starttls()
    server.login(me, 'offertool2w2016.')
    server.sendmail(me, receiver, msg.as_string())     
    server.close()
    
    
    return{'status':"Success"},200


@app.route('/forget_password',methods=['POST','GET'])
def forget_password():
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    email = request.args.get('email')
    try :
        if(re.fullmatch(pattern, email)):
            query_1='''select distinct(1) email from  user_management_ocp.user_details  where  email='{}' '''.format(email)   
            status=db.query(query_1)[0][0]
            receivers=email
            if(status==1):
                token = "rrrrr"
                email_sent_status = send_email(receivers, token)
                if email_sent_status :
                        return{"status":"Email sent successfully","status_code":200}
                else :
                        return{"status":"Error sending email to the user, Please try again later","status_code":500}
            else:
                return{"status":"Email does not exist","status_code":200}
        else :
            return{"status":"Please send the valid email address","status_code":500}
        

    except :
        return{"status":"Not-Exist Email","status_code":500}
    

if __name__ == '__main__':
   app.run()

