# from cryptography.fernet import Fernet


# key = Fernet.generate_key()
# fernet = Fernet(key)
# encoded_user_id = fernet.encrypt(user_id_str.encode()).decode()
# encrypte_new_password=fernet.encrypt(new_password_str.encode()).decode()

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

con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')


@app.route('/sidebar', methods=['GET','POST'])
def sidebar():
    username=request.args.get('user_name')
    
    query='''select menudescription as name,group_description as type,menudescription as tag  from  
    user_management_ocp.group G 
    inner join user_management_ocp.group_access_details A on G.groupid=A.groupid 
    inner join user_management_ocp.user_details U on G.groupid=U.group_id
    inner join  user_management_ocp.menu_item_details M on A.menuid=M.menuid
    inner join  user_management_ocp.submenu_item_details S on A."SubMenuID"=S."SubMenuID" where user_name='{}' '''.format(username)
    
    data=db.query(query)
    #df=pd.read_sql(query,con=con)
    #data=json.loads(df.to_json(orient='records'))
    name=data[0][0]
    url=''
    tag=data[0][1]
    type=data[0][2]
    print(name)
    
    query1='''select "SubMenuDescription" as name,route as url,icons as icon  from  
    user_management_ocp.group G 
    inner join user_management_ocp.group_access_details A on G.groupid=A.groupid 
    inner join user_management_ocp.user_details U on G.groupid=U.group_id
    inner join  user_management_ocp.menu_item_details M on A.menuid=M.menuid
    inner join  user_management_ocp.submenu_item_details S on A."SubMenuID"=S."SubMenuID" where user_name='{}' '''.format(username)
    
    df1=pd.read_sql(query1,con=con)
    df_json1=json.loads(df1.to_json(orient='records'))

    
    return{"name":name,"url":url,"tag":tag,"type":type,"children":df_json1}


if __name__ == '__main__':
    # app.run(host='172.16.4.190')
    app.run()