# -*- coding: utf-8 -*-
"""
Created on Wed Oct 20 09:56:42 2021
@author: subbu
"""
from flask import Flask, request, jsonify, make_response, request, render_template, session, flash,current_app
import jwt

from datetime import datetime, timedelta
from functools import wraps
import json
import psycopg2

from flask_cors import CORS


from datetime import datetime
from datetime import date
from Alloy_scrap import scrap_app
from smb_phase1 import smb_app1
from smb_phase2 import smb_app2
from smb_phase3 import smb_app3
from smb_history import smb_history
from taskbar1 import taskbar1
from web_api_response import web_api_response
import pandas as pd

from user_management import user_management_app

from taskbar_invoice import taskbar_invoice_app




app = Flask(__name__)
CORS(app)


app.register_blueprint(scrap_app)

app.register_blueprint(smb_app1)

app.register_blueprint(smb_app2)
app.register_blueprint(smb_app3)
app.register_blueprint(taskbar1)
app.register_blueprint(web_api_response)
app.register_blueprint(user_management_app)
app.register_blueprint(taskbar_invoice_app)
app.register_blueprint(smb_history)




con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')
cursor=con.cursor()


app.config['SECRET_KEY'] = 'YOU_SECRET_KEY'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1000 * 1000

# Login page


@app.route('/login', methods=['POST','GET'])
def login():
    username=request.args.get('username')
    password=request.args.get('password')
    
    
    
    try:
        
     query="select distinct(1) from  user_management_ocp.user_details  where user_name='{}' and password='{}'".format(username,password)
     
     
     cursor.execute(query)
     status=cursor.fetchall()[0][0]
     
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


       
 
if __name__ == '__main__':
    # app.run(host='172.16.4.190')
    app.run()
   