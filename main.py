# -*- coding: utf-8 -*-

from flask import Flask, request, jsonify, make_response, request, render_template, session, flash,current_app
import jwt
import json
import psycopg2
from smb_phase1 import Database
from flask_cors import CORS
from Alloy_scrap import scrap_app
from smb_phase1 import smb_app1
from smb_phase2 import smb_app2
from smb_phase3 import smb_app3
from smb_history import smb_history
from taskbar1 import taskbar1
import pandas as pd
from user_management import user_management_app
from taskbar_invoice import taskbar_invoice_app

from SMB_Generic import generic


app = Flask(__name__)
CORS(app)


app.register_blueprint(scrap_app)
app.register_blueprint(smb_app1)
app.register_blueprint(smb_app2)
app.register_blueprint(smb_app3)
app.register_blueprint(taskbar1)
app.register_blueprint(user_management_app)
app.register_blueprint(taskbar_invoice_app)
app.register_blueprint(smb_history)
app.register_blueprint(generic)


con1=psycopg2.connect(dbname='offertool',user='pgadmin',password='Sahara_17',host='offertool2-qa.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')

con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')
cursor=con.cursor()
app.config["environment_url"] = "https://smbprice.dcc-am.com/auth/reset-password?user_id"


app.config['SECRET_KEY'] = 'YOU_SECRET_KEY'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1000 * 1000



app.config['mypassword']='digitalway'


db=Database()

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


# @app.route("/SMBMinibarService",methods=['GET','POST'])
# def web_api():
    
#     data=str( request.data)
   
#     text_file = open("data.txt", "w")


#     text_file.write(data)
   
#     text_file.close()
#     data=json.loads(request.data)
    
#     if(str(data)[0]=='['):
#         data=data[0]
    
#     tableId = data['tableId']
#     data=data['data']
#     wherestr=''
#     db.insert("rollback")
#     query='select tablename from "SMB"."table_mapping" where id ={}'.format(tableId)
#     tableName=db.query(query)[0][0]
#     for i in list(data):
#         if(data[i]=="*"):
#             del data[i]
            
   
#     k=0
#     query='''select * from "SMB"."{}" where '''.format(tableName)
#     for i in data:
#         if(k==0):
#             wherestr=wherestr +'"' + i +'"' +" = " + "'" +str(data[i])+"'"
#             k=1
       
           
#         elif(k==1):
#             wherestr=wherestr + " and " +'"' + i +'"' +" = " + "'" +str(data[i])+"'"
#     query=query+wherestr
#     if(len(data)==0):
#         query=''' select * from "SMB"."{}" '''.format(tableName)
        
#     query+=" order by sequence_id asc limit 1"
#     print(query)
#     data=db.query(query)
       
#     df=pd.read_sql(query,con=con)
#     df_json=json.loads(df.to_json(orient='records'))
           
#     return {"data":df_json,"status":"sucess"}


@app.route("/SMBMinibarService",methods=['GET','POST'])
def web_api():
    
    data=str( request.data)
   
    text_file = open("data.txt", "w")


    text_file.write(data)
   
    text_file.close()
    data=json.loads(request.data)
    
    if(str(data)[0]=='['):
        data=data[0]
    
    tableId = data['tableId']
    data=data['data']
    wherestr=''
    db.insert("rollback")
    query='select tablename from "SMB"."table_mapping" where id ={}'.format(tableId)
    tableName=db.query(query)[0][0]
    
    k=0
    query='''select * from "SMB"."{}" where '''.format(tableName)
    for i in data:
        if(k==0):
            wherestr=wherestr+ "("+    '"' + i +'"'    +   " = "  +  "'" +str(data[i])+"'"   + " or " +   '"' + i +'"' + " = "+ "'"+ "*" +"'"+     ")"
            k=1
          
           
        elif(k==1):
            wherestr=wherestr + " and " +"("+    '"' + i +'"'    +   " = "  +  "'" +str(data[i])+"'"   + " or " +   '"' + i +'"' + " = "+ "'"+ "*" +"'"+     ")"
            
    query=query+wherestr
    if(len(data)==0):
        query=''' select * from "SMB"."{}" '''.format(tableName)
        
    query+=" order by sequence_id asc limit 1"
    print(query)
    data=db.query(query)
       
    df=pd.read_sql(query,con=con1)
    df_json=json.loads(df.to_json(orient='records'))
           
    return {"data":df_json,"status":"sucess"}
   
        
        
 
if __name__ == '__main__':
    app.run(host='172.16.4.190')
    # app.run()
   