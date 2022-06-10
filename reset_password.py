
from flask import Flask, request, send_file, render_template, make_response
from flask import jsonify, Blueprint, current_app
from flask_cors import CORS

import psycopg2
from sqlalchemy import create_engine
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import re
from flask import Flask
from smb_phase1 import Database
app= Flask(__name__)
reset_password = Blueprint('reset_password', __name__)
CORS(reset_password)
engine = create_engine('postgresql://postgres:ocpphase01@ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com:5432/offertool')

db=Database()
reset_password = Blueprint('reset_password', __name__)
CORS(reset_password)

# con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')
# cursor=con.cursor()



def send_email(email,fname,user_id):
    me='''subrahamanya@digitalway-lu.com'''
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "Reset Password"
    msg['From'] = me
    msg['To'] =email
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
        <th>Hello&nbsp;'''+fname+'''</th>
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
                <a href="''' + current_app.config["environment_url"] + '''={}&email={}"><u> Reset Password <u></a>
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
</html>'''.format(user_id,email)
    
    
    part = MIMEText(html, 'html')
    msg.attach(part)
    server = smtplib.SMTP('smtp.gmail.com',587)
    server.ehlo()
    server.starttls()
    server.login(me, '78df@$M80')
    server.sendmail(me, email, msg.as_string())     
    server.close()
    
    
    return "success"


@reset_password.route('/resetemail',methods=['POST','GET'])
def resetemail():
   
             email = request.args.get('email')
   

    
   
      
             query=''' select user_name,user_id from user_management_ocp.user_details where email='{}' '''.format(email)
             try:
              username=db.query(query)[0][0]
              user_id=db.query(query)[0][1]
             except:
                 username=0
                 
             if(username!=0):
                 send_email(email,username,user_id)
                 return {"status":"success"},200
             else:
                return {"status":"failure"},500
            
             
                 
                 
                 
@reset_password.route('/reset_password',methods=['POST','GET'])
def resetPassword():
    new_password=request.args.get('NewPassword')
    confirm_password=request.args.get('ConfirmPassword')
    email=request.args.get('email')
    user_id=request.args.get('user_id')
    
    
    if(confirm_password==new_password):
        
        query=''' Update user_management_ocp.user_details set password='{}' where user_id='{}' and email='{}' '''.format(new_password,user_id,email)
        print(query)
        db.insert(query)
        return {"status":"success"},200
    else:
        return {"status":"failure"},500
    
    