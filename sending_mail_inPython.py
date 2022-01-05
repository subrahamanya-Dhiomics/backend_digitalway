# -*- coding: utf-8 -*-
"""
Created on Tue Jan  4 09:17:39 2022

@author: Administrator
"""
import smtplib

sender = 'ymsyathish@gmail.com'
receivers = ['yathish.susandhi@gmail.com']

message = """From: No Reply <no_reply@mydomain.com>
To: Person <yathish.susandhi@gmail.com>
Subject: reset passoword

This is a test e-mail message.
Link:https://smbprice.dcc-am.com/auth/reset-password


"""
try:

    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo()
    server.starttls()
    server.login(sender, 'offertool2w2016.')
    server.sendmail(sender, receivers, message)
    server.close()
    
    print ('successfully sent the mail')
except:
        print ("failed to send mail")
   
'''   smtpObj.sendmail(sender, receivers, message)         
   print("Successfully sent email")
except smtplib.SMTPException:
   print("Error: unable to send email")
   print(smtplib.SMTPException)
'''