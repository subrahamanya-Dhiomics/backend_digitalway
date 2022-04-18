# -*- coding: utf-8 -*-
"""
Created on Mon Jan 24 11:27:26 2022

@author: User
"""

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

def send_email():
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
        <th>Hello&nbsp;Devendra&nbsp;P&nbsp;Singh,</th>
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
                <a href="google.com={}&email={}"><u> Reset Password <u></a>
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
</html>'''

    
    # me='''devendra@digitalway-lu.com'''
    # password="dhiOmics@2021"
    # msg = MIMEMultipart('alternative')
    # msg['Subject'] = "Sample Reset Password Email"
    # msg['From'] = me
    # msg['To'] = "devendra@digitalway-lu.com"
    # part = MIMEText(html, 'html')
    # msg.attach(part)
    
    me='''EUROPE\ELPSMBUP'''
    password="AM@prictab01*"
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "Sample Reset Password Email"
    msg['From'] = me
    msg['To'] = "subrahamanya.shetty@dhiomics.com"
    part = MIMEText(html, 'html')
    msg.attach(part)
    
    # me='''singhpdevendra@gmail.com'''
    # password="7829847182#Dev"
    # msg = MIMEMultipart('alternative')
    # msg['Subject'] = "Sample Reset Password Email"
    # msg['From'] = me
    # msg['To'] = "devendra@digitalway-lu.com"
    # part = MIMEText(html, 'html')
    # msg.attach(part)
    

    try:
        
        with smtplib.SMTP(host="outlook.office365.com", port=587) as smtp_obj:
        # with smtplib.SMTP(host="smtp-mail.outlook.com", port=587) as smtp_obj:
        # with smtplib.SMTP(host="smtp.gmail.com", port=587) as smtp_obj:#, port=587
            print("smtp obj created")
            smtp_obj.ehlo()
            smtp_obj.starttls()
            smtp_obj.ehlo()
            smtp_obj.login(me, password)
            smtp_obj.sendmail(msg['From'], [msg['To'],], msg.as_string())
    except Exception as ex:
        print("Error",ex)

