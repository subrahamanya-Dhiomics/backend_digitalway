# -*- coding: utf-8 -*-
"""
Created on Thu Oct  12 07:33:38 2021
@author: subbu
"""


# import section (python libraries)

from flask import Blueprint
import pandas as pd
import json
from flask import Flask, request, send_file, render_template, make_response,current_app
from flask import jsonify
from flask_cors import CORS
from json import JSONEncoder

from SendMailTest import send_email

import psycopg2
import shutil
from functools import wraps
from pathlib import Path
import os
from sqlalchemy import create_engine
import getpass
import cryptocode
from datetime import datetime,date
import jwt
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText



# database connection strings ( ocpphase1 -- > offertool >SMB)

engine = create_engine('postgresql://postgres:ocpphase01@ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com:5432/offertool')

con = psycopg2.connect(dbname='offertool',user='pgadmin',password='Sahara_17',host='offertool2-qa.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')

#database class for updating and fetching the data 
class Database:
    host='offertool2-qa.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com'  # your host
    user='pgadmin'      # usernames
    password='Sahara_17' 
    db='offertool'
    
    
    
    def __init__(self):
            
            self.connection = psycopg2.connect(dbname=self.db,user=self.user,password=self.password,host=self.host)
             
    def insert(self, query):
           
            var = 'failed'
            try:
                self.cursor = self.connection.cursor()
#                print("HEY")
                var = self.cursor.execute(query)
               
                self.connection.commit()
            except:
                self.connection.rollback()
            finally:
                self.cursor.close()
               
            return(var)

    def query(self, query):
        try:
            self.cursor = self.connection.cursor()
           
            self.cursor.execute(query)
            return self.cursor.fetchall()
        finally:
            self.cursor.close()
           

    
 
# flsk app declaration 

smb_app1 = Blueprint('smb_app1', __name__)
CORS(smb_app1)
db=Database()


def get_required_columns(cols):
    rem=['aprover1','aprover2','aprover3','sequence_id','active']
    
    for r in rem:
        try:
         cols.remove(r)
        except:
            pass
    return  cols
def move_deleted_record():
    return 'dsf'

#user defined functions section 
# 1)token generation
# 2)tuple to string (return columns in string)
# 3)upsert ( to upsert the changed data into a master table)
# 4)move records ( to move the data from master to main and main to history)
# 5)email ( to send an email with html template )


def token_required(func):
    # decorator factory which invoks update_wrapper() method and passes decorated function as an argument
    @wraps(func)
    def decorated(*args, **kwargs):
        #token = request.args.get('token')
        #if 'x-access-token' in request.headers:
        token = request.headers['x-access-token']  
       
        try:
            data = jwt.decode(token, current_app.config['SECRET_KEY'], algorithms=["HS256"])
            print(data)
        except :      
             return {"msg":"Invalid token;"}
        return func(*args, **kwargs)
    return decorated




def tuple_to_string(col_tuple):
    columnstr=''' '''
    for i in col_tuple:
             if(i!=col_tuple[-1]):  columnstr+=''' "{}",'''.format(i)
             else: columnstr+=''' "{}" '''.format(i)   
    return columnstr

def highlight_col(x):
    r = 'background-color: grey'
    df1 = pd.DataFrame('', index=x.index, columns=x.columns)
    df1.loc[:,['id']] = r
    return df1

    
def test(col_tuple,input_tuple,id_value,tablename):
    
    print(col_tuple)
    print(input_tuple)
    sequence_id=''
    col_tuple=list(col_tuple)
    input_tuple=list(input_tuple)
    lis=["table_name","id","Username"]
    
    for n in lis:   
     col_tuple.remove(n)
   
    input_tuple.pop(0)
    input_tuple.pop(0)
    input_tuple.pop(1)
        
    tag=False
    columnstr=tuple_to_string(col_tuple)
    
    query=''' select {} from "SMB"."{}" where id={} '''.format(columnstr,tablename,id_value)
    
   
    dat=list(db.query(query)[0])
    
    
    if(dat[0]!=input_tuple[0]):
    
        tag=False
        sequence_id=input_tuple[0]
        
        dat.pop(0)
        input_tuple.pop(0)
        if(input_tuple==dat):
            tag=True
    
    
    return {"tag":tag,"seq":sequence_id}
            

def move_direct(tablename,id_value,sequence_id):
    
        print("inside move_direct")
        
    
      # sequence_id logic
        sequence_initial=db.query(''' select sequence_id from "SMB"."{}" where id={} and active=1 '''.format(tablename,id_value) )[0][0]
        vacant_check=0
       
        try:
         vacant_check=db.query('''select active from "SMB"."{}" where sequence_id={} and active=1 '''.format(tablename,sequence_id))[0][0]
        except:
          pass
     
        if(vacant_check==1):
            sequence_final=int(sequence_id)
            if(sequence_final<sequence_initial):
                id_array=db.query(''' select id from "SMB"."{}" where sequence_id >={} and sequence_id<{} and active=1 order by sequence_id '''.format(tablename,sequence_final,sequence_initial))
                id_array=list(sum(id_array,()))
                for i in  id_array:
                 qr1= ''' UPDATE "SMB"."{}" SET sequence_id=sequence_id+1 where id={}  and active=1 '''.format(tablename,i)
                 print(qr1)
                 db.insert(qr1)
            else:
                id_array=db.query(''' select id from "SMB"."{}" where sequence_id >{} and sequence_id<={} and active=1 order by sequence_id '''.format(tablename,sequence_initial,sequence_final))
                id_array=list(sum(id_array,()))
                for j in  id_array:
                 qr2= ''' UPDATE "SMB"."{}" SET sequence_id=sequence_id-1 where id={} and active=1 '''.format(tablename,j)
                 db.insert(qr2)
        query2= ''' UPDATE "SMB"."{}" SET sequence_id={} where id={}  '''.format(tablename,sequence_id,id_value)
        print(sequence_initial)
        
        print(vacant_check)
        print(tablename)
        print("*****")
        db.insert(query2)
        
        return True
       
    
    
            
        
         
        
        
        
   
    

def upsert(col_tuple=None,input_tuple=None,flag='update',tablename=None,id_value=None):
        querystr=''' '''
        columnstr=tuple_to_string(col_tuple)
        tableid=''
        
        now = datetime.now()
        date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
        
        
       
        
        if(flag=='update'):
            
            
            test_case=test(col_tuple,input_tuple,id_value,tablename)
             
            print(test_case)
            
            if(test_case['tag']==True):
               stat=move_direct(tablename,id_value,test_case['seq'])
               status='move_direct'
            else:
                check=db.query(''' select exists(select 1  from "SMB"."SMB_Aproval" where id={} and table_name='{}') '''.format(id_value,tablename))[0][0]
                if check and check!='failed':querystr=''' UPDATE "SMB"."SMB_Aproval" SET ({})= {} where id={} and table_name='{}' '''.format(columnstr,input_tuple,id_value,tablename)
                else: querystr='''INSERT INTO "SMB"."SMB_Aproval" ({}) VALUES{}; '''.format(columnstr,input_tuple)
                 
               
                
                status=db.insert(querystr)
                print(querystr)
                tableid=db.query(''' select max(tableid) from "SMB"."SMB_Aproval" where id={} and table_name='{}' '''.format(id_value,tablename))[0][0]
                time_stamp_update=db.insert(''' update "SMB"."SMB_Aproval"  set updated_on='{}' where tableid={} '''.format(date_time,tableid))
               
        if(flag=='add'):
            querystr=''' Insert into  "SMB"."SMB_Aproval"  ({}) VALUES{}'''.format(columnstr,input_tuple)
           
            status=db.insert(querystr)
            print(querystr)            
            
            tableid=db.query(''' select max(tableid) from "SMB"."SMB_Aproval" where table_name='{}' and flag='add' '''.format(tablename) )[0][0]
            time_stamp_update=db.insert(''' update "SMB"."SMB_Aproval"  set updated_on='{}' where tableid={} '''.format(date_time,tableid))
               
        
        
     
        if status!='failed' and status!='move_direct'  : status='success'
        return {"status":status,"tableid":tableid}
    
def move_records(tablename,col_tuple,value_tuple,flag,id_value=None,sequence_id=None):
    querystr=''' '''
    columnstr=tuple_to_string(col_tuple)
    
    col_list=list(col_tuple)
    if("Username" not in col_list):
     col_list.append("Username")
    col_list.remove("aprover1")
    al_columnstr=tuple_to_string(tuple(col_list))
    
    
    
    # case for updating the record
    
    if(flag=='update'):
        # moving the records to history from main table
        
        query1=''' INSERT INTO "SMB"."{}_History"({})
        SELECT 
        
        {}
    
         FROM "SMB"."{}"
        WHERE "id" ={} '''.format(tablename,al_columnstr,al_columnstr,tablename,id_value)
        
        
        # sequence_id logic
        sequence_initial=db.query(''' select sequence_id from "SMB"."{}" where id={} and active=1 '''.format(tablename,id_value) )[0][0]
        vacant_check=0
        try:
         vacant_check=db.query('''select active from "SMB"."{}" where sequence_id={} '''.format(tablename,sequence_id))[0][0]
        except:
          pass
     
        if(vacant_check==1):
            sequence_final=int(sequence_id)
            if(sequence_final<sequence_initial):
                id_array=db.query(''' select id from "SMB"."{}" where sequence_id >={} and sequence_id<{} and active=1 order by sequence_id '''.format(tablename,sequence_final,sequence_initial))
                id_array=list(sum(id_array,()))
                for i in  id_array:
                 qr1= ''' UPDATE "SMB"."{}" SET sequence_id=sequence_id+1 where id={}  and active=1 '''.format(tablename,i)
                 print(qr1)
                 db.insert(qr1)
            else:
                id_array=db.query(''' select id from "SMB"."{}" where sequence_id >{} and sequence_id<={} and active=1 order by sequence_id '''.format(tablename,sequence_initial,sequence_final))
                id_array=list(sum(id_array,()))
                for j in  id_array:
                 qr2= ''' UPDATE "SMB"."{}" SET sequence_id=sequence_id-1 where id={} and active=1 '''.format(tablename,j)
                 db.insert(qr2)
                
        
        # updating the record in main table 
        query2= ''' UPDATE "SMB"."{}" SET ({})= {} where id={}  '''.format(tablename,columnstr,value_tuple,id_value)
        print(query1)
        print(query2)
        db.insert(query1)
        status=db.insert(query2)
    
        if(status!='failed'):
            db.insert(''' delete from "SMB"."SMB_Aproval" where id={} '''.format(id_value))
            status='success'
        return status
      
        
    # case for adding the record
    if(flag=='add'):
        
        print(columnstr)
        print(value_tuple)
        print("*******")
        query='''INSERT INTO "SMB"."{}"(
             
            {})
             VALUES{};'''.format(tablename,columnstr,value_tuple)
        
        print(query)
        status=db.insert(query)
        if(status!='failed'):
            db.insert(''' delete from "SMB"."SMB_Aproval" where tableid={} '''.format(id_value))
            status='success'     
        return status
    

def email(id_value,tablename,flag='change',username='admin'):  
          
       
        
        encoded_id = cryptocode.encrypt(str(id_value),current_app.config["mypassword"])
        ## And then to decode it:
        
        approver='arriluceaz@gmail.com'
        mail_from='''subrahamanya@digitalway-lu.com'''
        
       
        msg = MIMEMultipart('alternative')
        msg['Subject'] = "Approve Records"
        msg['From'] =mail_from
        msg['To'] = approver
        #print(user)
        html=''
        with open('/home/ubuntu/SMBPricingAPI/backend_digitalway/email_aproval.html', 'r') as f:
         html = f.read()
        
        html=html.format(tablename,username,id_value,encoded_id,tablename,flag)

        part = MIMEText(html, 'html')
        msg.attach(part)
        server = smtplib.SMTP('smtp.gmail.com',587)
        server.ehlo()
        server.starttls()
        server.login(mail_from, '78df@$M80')
        server.sendmail(mail_from, approver, msg.as_string())     
        server.close()
        return 'success'
   

# delcaring the paths

# download_path=input_directory="C:/Users/Administrator/Documents/"
download_path=input_directory="/home/ubuntu/mega_dir/"



# llist of generinc api's works for all 21 tables
     
@smb_app1.route('/aproval_data',methods=['GET','POST'])
def aproval_data():
    id_value=request.args.get('id_value')
    flag=request.args.get('flag')
    
    # decoding the id array 
    id_value=id_value.replace(' ','+')
    id_value = cryptocode.decrypt(id_value, "digitalway")
    id_list=eval(id_value)
    id_list.append(0)
    id_tuple=tuple(id_list)
    tablename=request.args.get('tablename')
    
    # preparing the columns list
    col_df=pd.read_sql(''' select * from "SMB"."{}"  limit 1 '''.format(tablename),con)
    lis=list(col_df.columns)
    lis.append('flag')
    removals=['id','Username','active','aprover1','aprover2','aprover3']
    for i in removals:
        lis.remove(i)
        
        
    if(flag=='delete'):
        
        query=''' select * from "SMB"."{}" where id in {} and active=1 '''.format(tablename,id_tuple)
        
        df=pd.read_sql(query,con)
        df.insert(0,"flag",flag)
        df['updated_on'] = df['updated_on'].astype('datetime64[s]')
        df['updated_on']=pd.to_datetime(df['updated_on'])
        df['updated_on']=df['updated_on'].astype(str)
        data=json.loads(df.to_json(orient='records'))
        
        return {"data":data,"lis":lis},200

    else:

        query='''select * from "SMB"."SMB_Aproval" where tableid in {} and table_name='{}' '''.format(id_tuple,tablename)
       
        id_data=db.query(''' select id from "SMB"."SMB_Aproval" where tableid in {} and table_name='{}' '''.format(id_tuple,tablename))
        id_data=sum(id_data,())
        id_data=list(id_data)
        id_data.append(0)
        
        old_json=[]
        
        if(None not in id_data and len(id_data)>1):
       
         old_data_df=pd.read_sql(''' select * from "SMB"."{}" where id in {} '''.format(tablename,tuple(id_data)),con)
         
         old_data_df['updated_on'] = old_data_df['updated_on'].astype('datetime64[s]')
         old_data_df['updated_on']=pd.to_datetime(old_data_df['updated_on'])
         old_data_df['updated_on']=old_data_df['updated_on'].astype(str)
         old_json=json.loads(old_data_df.to_json(orient='records'))
       
        
        df=pd.read_sql(query,con=con)
       
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Zip_Code_(Dest)":"Zip_Code_Dest"},inplace=True)
        df.dropna(axis=1, how='all',inplace=True)
        try:
            df['updated_on'] = df['updated_on'].astype('datetime64[s]')
            df['updated_on']=pd.to_datetime(df['updated_on'])
            df['updated_on']=df['updated_on'].astype(str)
        except:
            pass
        data=json.loads(df.to_json(orient='records'))
        
        return {"data":data,"lis":lis,"old_json":old_json},200
      
 

@smb_app1.route('/aprove_records',methods=['GET','POST'])
def aprove_records():
    data=json.loads(request.data)
    df=pd.DataFrame(data["data"])
    tablename=data['tablename']
    email=data['email']
    
    print(data)
    
    
    if(df['flag'][0]=='delete'):
        
           
            id_value=list(df['id'])
            id_value.append(0)
            id_value=tuple(id_value)
                        
            query='''UPDATE "SMB"."{}" SET "active"=0 WHERE "id" in {} '''.format(tablename,id_value)
           
            result=db.insert(query)
            df=pd.read_sql('''select * from "SMB"."{}" limit 1'''.format(tablename),con)
            cols=get_required_columns(list(df.columns))
            
            col_string=tuple_to_string(tuple(cols))
            
            query='''insert into "SMB"."{}_History"({}) select {} from 
"SMB"."{}" where "id" in {} '''.format(tablename,col_string,col_string,tablename,id_value)
            
            
            
            result=db.insert(query)
            
            return {"status":"success"},200
        
        
        
    else:
        df.insert(0,"aprover1",email)
        for i in range(0,len(df)):
            flag=df['flag'][i]
             
            tableid=df['tableid'][i]
            try:
                id_value=df['id'][i]
                sequence_id=df['sequence_id'][i]
                
            except:
                pass
            
            if(flag=='update'):
             df2=df.drop(['tableid','flag','status','table_name'], axis = 1)
             col_tuple=tuple(list(df2.columns))
             value_tuple=tuple(df2.loc[i])
           
             status=move_records(tablename,col_tuple,value_tuple,flag,id_value,sequence_id)
           
            else:
               df2=df.drop(['tableid','flag','status','table_name'], axis = 1)
               try:
                   
                sequence_id=db.query(''' select max(sequence_id) from "SMB"."{}" where "active"=1 '''.format(tablename))[0][0]+1
               except:
                   sequence_id=1
               df2.insert(0,"sequence_id",sequence_id)
               
               col_tuple=tuple(list(df2.columns))
               value_tuple=tuple(df2.loc[i])
               status=move_records(tablename,col_tuple,value_tuple,flag,tableid)
            
        return {"status":"success"},200

@smb_app1.route('/reject_records',methods=['GET','POST'])
def reject_records():
    data=json.loads(request.data)
    tablename=data['tablename']
    id_value=data['id_value']
    id_value=id_value.replace(' ','+')
    id_value = cryptocode.decrypt(id_value, current_app.config["mypassword"])
    id_list=eval(id_value)
    id_list.append(0)
    id_tuple=tuple(id_list) 
    query=''' delete from "SMB"."SMB_Aproval" where tableid in {} '''.format(id_tuple)
    db.insert(query)
    return {"status":"success"},200
    

# normal apis for crud operations * 21

@smb_app1.route('/Base_Price_Data',methods=['GET','POST'])
@token_required
def SMB_data():
    
    # query_paramters 
    search_string=request.args.get("search_string") 
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    send_email()
    
    if(limit==None):
        limit=500
    if(offset==None):
        offset=0
    

    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    # fetching the data from database and filtering    
    try:
        query='''select * from "SMB"."SMB - Base Price - Category Addition" where "active"='1' order by sequence_id  OFFSET {} LIMIT {} '''.format(lowerLimit,upperLimit) 
        df = pd.read_sql(query, con=con)
        
        count=db.query('select count(*) from "SMB"."SMB - Base Price - Category Addition" where "active"=1 ')[0][0]
        
        
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

    
    
   
   
@smb_app1.route('/get_record_baseprice',methods=['GET','POST'])  
@token_required     
def get_record():
    id_value=request.args.get('id')  
    
    query='''select * from "SMB"."SMB - Base Price - Category Addition" where "id"={} '''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500



@smb_app1.route('/update_record_baseprice',methods=['POST'])
@token_required
def update_record1():
        username=request.headers['username']
       
        now = datetime.now()
        date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
        
        query_parameters =json.loads(request.data) 
        
        sequence_id=(query_parameters['sequence_id'])
        BusinessCode=(query_parameters['BusinessCode'])
        Market_Country =( query_parameters["Market_Country"])
        Product_Division =( query_parameters["Product_Division"])
        Product_Level_02 =( query_parameters["Product_Level_02"])
        Document_Item_Currency =( query_parameters["Document_Item_Currency"])
        Amount =( query_parameters["Amount"])
        Currency =( query_parameters["Currency"])
        id_value=(query_parameters['id_value'])
        sequence_id=(query_parameters['sequence_id'])
        
        
        flag='update'
      
        tablename='SMB - Base Price - Category Addition'        
        
        input_tuple=(tablename,id_value,sequence_id,username, BusinessCode,  Market_Country, Product_Division,Product_Level_02,Document_Item_Currency, Amount,Currency)
        
        col_tuple=("table_name","id","sequence_id",
             "Username",
             "BusinessCode",
             "Market - Country",
             "Product Division",
             "Product Level 02",
             "Document Item Currency",
             "Amount",
             "Currency")
        email_status=''
        
        status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
        if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)
        
        
        
        if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
        else: return {"status":"failure"},500


@smb_app1.route('/delete_record_baseprice',methods=['POST','GET'])
@token_required

def delete_record():  
    id_value,username=json.loads(request.data)['id'],request.headers['username']
    
    tablename='SMB - Base Price - Category Addition'
   
    status=email(id_value,tablename,'delete',username=username)
    
    if(status=='success'):return {"status":"success"},200
    else: return {"status":"failure"},500


     
@smb_app1.route('/add_record_baseprice',methods=['POST'])
@token_required
def add_record1():
    username=request.headers['username']
    
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    
    query_parameters =json.loads(request.data)
    BusinessCode=(query_parameters['BusinessCode'])
    Market_Country =( query_parameters["Market_Country"])
    Product_Division =( query_parameters["Product_Division"])
    Product_Level_02 =( query_parameters["Product_Level_02"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    flag='add'
    
    
    
    
    tablename='SMB - Base Price - Category Addition'
    # id_value=db.query('''select "SMB".get_max_id('"SMB"."SMB - Base Price - Category Addition"')''')[0][0]+1
    
    
    
    input_tuple=(tablename,flag,username, BusinessCode,  Market_Country, Product_Division,Product_Level_02,Document_Item_Currency, Amount,Currency)
    col_tuple=("table_name","flag", "Username",
              "BusinessCode",
              "Market - Country",
              "Product Division",
              "Product Level 02",
              "Document Item Currency",
              "Amount",
              "Currency")
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
  
   
@smb_app1.route('/Base_Price_Upload', methods=['GET','POST'])

def  SMB_upload():
    
            f=request.files['filename']
      
                
            f.save(input_directory+f.filename)
            
            
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[['id','BusinessCode','Market - Country','Product Division','Product Level 02','Document Item Currency', 'Amount', 'Currency']]  
            df['id']=df['id'].astype(int)
            
            
            df_main = pd.read_sql('''select "id","BusinessCode","Market - Country","Product Division","Product Level 02","Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Base Price - Category Addition" where "active"='1' order by "id" ''', con=con)
            
            
            df['Currency'] = df['Currency'].str.replace("'","")
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df['Currency'] = df['Currency'].str.replace("'","")
            
            
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)  
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200

def fun_ignore_sequence_id(df,tablename):
     
     id_tuple=list(df['id'])
     id_tuple.append(0)
     sequence_id=db.query(''' select sequence_id from "SMB"."{}" where id in {} '''.format(tablename,tuple(id_tuple)))
     df.insert(0,'sequence_id',list(sum(sequence_id, ())))
     
     return df
         

@smb_app1.route('/Base_Price_validate', methods=['GET','POST'])
@token_required
def  SMB_validate():
    json_data=json.loads(request.data)
    username=request.headers['username']
     
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    email_status=''
    df.insert(0,'Username',username)
    
    
    
   
    
    tablename='SMB - Base Price - Category Addition' 
    df=fun_ignore_sequence_id(df,tablename)
    flag='update'  
    df.insert(1,'table_name',tablename)
    col_tuple=("table_name","id","sequence_id",
             "Username",
             "BusinessCode",
             "Market - Country",
             "Product Division",
             "Product Level 02",
             "Document Item Currency",
             "Amount",
             "Currency")
    col_list=['table_name','id','sequence_id','Username','BusinessCode','Market_Country','Product_Division','Product_Level_02','Document_Item_Currency', 'Amount', 'Currency']
    
    
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
  
    return {"status":"success"},200
   
     

         
@smb_app1.route('/Base_price_download',methods=['GET'])
def SMB_baseprice_download1():
   
        now = datetime.now()
       
        df = pd.read_sql('''select "id",sequence_id,"BusinessCode",
             "Market - Country",
             "Product Division",
             "Product Level 02",
             "Document Item Currency",
             "Amount",
             "Currency" from "SMB"."SMB - Base Price - Category Addition" where "active"='1' order by sequence_id ''', con=con)
        
        t=now.strftime("%d-%m-%Y-%H-%M-%S")
        
        file=download_path+t+'baseprice_category_addition.xlsx'
        
        df=df.style.apply(highlight_col, axis=None)
        df.to_excel(file,index=False)
        
        return send_file(file, as_attachment=True)
       
        
# ***************************************************************************************************************************************************************************************
# baseprice_minibar

@smb_app1.route('/data_baseprice_category_minibar',methods=['GET','POST'])
@token_required
def SMB_data_baseprice_mini():
    # query_paramters 
    search_string=request.args.get("search_string")  
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)

    if(limit==None):
        limit=500
    if(offset==None):
        offset=0

    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
   
    # fetching the data from database and filtering    
    try:
        query='''select * from "SMB"."SMB - Base Price - Category Addition - MiniBar" where "active"='1' order by sequence_id  OFFSET {} LIMIT {} '''.format(lowerLimit,upperLimit) 
        df = pd.read_sql(query, con=con)
        
        
        count=db.query('select count(*) from "SMB"."SMB - Base Price - Category Addition - MiniBar" where "active"=1 ')[0][0]
        
        
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
  
    
  
@smb_app1.route('/delete_record_baseprice_category_minibar',methods=['POST','GET','DELETE'])
@token_required
def delete_record_baseprice_mini():  
    id_value,username=json.loads(request.data)['id'],request.headers['username']
    print(username,id_value)
    
    
    tablename='SMB - Base Price - Category Addition - MiniBar'
   
    status=email(id_value,tablename,'delete',username=username)
    return {"status":"success"},200




@smb_app1.route('/get_record_baseprice_category_minibar',methods=['GET','POST'])     
@token_required  
def get_record_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Base Price - Category Addition - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    

@smb_app1.route('/update_record_baseprice_category_minibar',methods=['POST'])
@token_required
def update_record_category_minibar():
        username=request.headers['username']
    
        now = datetime.now()
        date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
        query_parameters =json.loads(request.data) 
        
        BusinessCode=(query_parameters['BusinessCode'])
        Customer_Group=(query_parameters["Customer_Group"])
       
        Market_Country =( query_parameters["Market_Country"])
        Beam_Category=(query_parameters["Beam_Category"])
        Document_Item_Currency =( query_parameters["Document_Item_Currency"])
        Amount =( query_parameters["Amount"])
        Currency =( query_parameters["Currency"])
        sequence_id=(query_parameters['sequence_id'])
        id_value=(query_parameters['id_value'])
        Market_Customer=(query_parameters['Market_Customer'])
        
        
        
        flag='update'
      
        tablename='SMB - Base Price - Category Addition - MiniBar'     
        
        input_tuple=(tablename,id_value,sequence_id,username,BusinessCode,Customer_Group,Market_Customer,Market_Country,Beam_Category,Document_Item_Currency, Amount,Currency)
        
        col_tuple=("table_name","id","sequence_id",
              "Username",
              "BusinessCode",
              "Customer Group",
              "Market - Customer",
             
              "Market - Country",
              "Beam Category",
              "Document Item Currency",
              "Amount",
              "Currency")
        email_status=''
        
        status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
        if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)
        
        
        
        if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
        else: return {"status":"failure"},500
   


@smb_app1.route('/add_record_baseprice_category_minibar',methods=['POST'])
@token_required
def add_record_mini():
    
    today = date.today()
    username=request.headers['username']
    
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Customer_Group=(query_parameters["Customer_Group"])
    
    Market_Country =( query_parameters["Market_Country"])
    
    Beam_Category=(query_parameters["Beam_Category"])
    Market_Customer=(query_parameters["Market_Customer"])
    
    
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    flag='add'
   
    
    tablename='SMB - Base Price - Category Addition - MiniBar'     
   
    input_tuple=(tablename,flag,username, BusinessCode,Customer_Group,Market_Customer,  Market_Country,Beam_Category,Document_Item_Currency, Amount,Currency)
    col_tuple=("table_name","flag",  "Username",
             
             "BusinessCode",
             "Customer Group",   
             "Market - Customer",
             "Market - Country",
        	 "Beam Category",
             "Document Item Currency",
             "Amount",
             "Currency")
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
       
   
@smb_app1.route('/upload_baseprice_category_minibar', methods=['GET','POST'])

def  SMB_upload_baseprice_minibar():
            print("step1 : here")
            request.files['filename']
            
            f=request.files['filename']
            print("step2 ",f.filename)
            
             
            
            
            f.save(input_directory+f.filename)
            
            print("step3",type(f))
        
        
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[['id','BusinessCode', 'Customer Group','Market - Customer', 'Market - Country', 'Beam Category','Document Item Currency', 'Amount', 'Currency','sequence_id']]  
            df['id']=df['id'].astype(int)
            df["sequence_id"]=df["sequence_id"].astype(int)
           
            df_main = pd.read_sql('''select "id","BusinessCode", "Customer Group", "Market - Customer","Market - Country", "Beam Category","Document Item Currency", "Amount", "Currency",sequence_id from "SMB"."SMB - Base Price - Category Addition - MiniBar" where "active"='1' order by sequence_id ''', con=con)
            
            
            df['Currency'] = df['Currency'].str.replace("'","")
            
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
       


@smb_app1.route('/validate_baseprice_category_minibar', methods=['GET','POST'])
@token_required
def  SMB_validate_baseprice_minibar():
    json_data=json.loads(request.data)
    username=request.headers['username']
     
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    tablename='SMB - Base Price - Category Addition - MiniBar'     
    
      
    flag='update'  
    df.insert(1,'table_name',tablename)
    df.insert(2,'Username',username)
    col_tuple=("table_name","id","sequence_id",
            "BusinessCode",
             "Customer Group",
             "Market - Customer",
            
             "Market - Country",
        	 "Beam Category",
             "Document Item Currency",
             "Amount",
             "Currency")
    col_list=['table_name','id','sequence_id','BusinessCode','Customer_Group','Market_Customer','Market_Country','Beam_Category','Document_Item_Currency', 'Amount', 'Currency']
    
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
  
    return {"status":"success"},200
   
  
         
@smb_app1.route('/download_baseprice_category_minibar',methods=['GET'])

def SMB_baseprice_catecory_minibar_download():
   
        now = datetime.now()
        try:
           
            df = pd.read_sql('''select "id",sequence_id, "BusinessCode",
              "Customer Group",
              "Market - Customer",
             
              "Market - Country",
              "Beam Category",
              "Document Item Currency",
              "Amount",
              "Currency" from "SMB"."SMB - Base Price - Category Addition - MiniBar" where "active"='1' order by sequence_id ''', con=con)
            
            
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            
            file=download_path+t+'baseprice_addition_minibar.xlsx'
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
           
            
        except:
            return {"status":"failure"},500
    
    
# *******************************************************************************************************************************************************************
# incoterm exceptions

@smb_app1.route('/data_baseprice_incoterm',methods=['GET','POST'])

def SMB_data_baseprice_incoterm():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    
    if(limit==None):
        limit=500
    if(offset==None):
        offset=0
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    
    # fetching the data from database and filtering    
    try:
        query='''select * from "SMB"."SMB - Base Price - Incoterm Exceptions" where "active"='1' order by sequence_id OFFSET {} LIMIT {} '''.format(lowerLimit,upperLimit) 
        df=pd.read_sql(query,con=con )
        count=db.query('select count(*) from "SMB"."SMB - Base Price - Incoterm Exceptions" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

@smb_app1.route('/delete_record_baseprice_incoterm',methods=['POST','GET','DELETE'])
@token_required
def delete_record_baseprice_incoterm():  
    
   
    
    id_value,username=json.loads(request.data)['id'],request.headers['username']
    
    
    tablename='SMB - Base Price - Incoterm Exceptions'
   
    status=email(id_value,tablename,'delete',username=username)
    if(status=='success'):return {"status":"success"},200
    else: return {"status":"failure"},500
    


@smb_app1.route('/get_record_baseprice_incoterm',methods=['GET','POST'])     
@token_required  
def get_record_incoterm():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Base Price - Incoterm Exceptions" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app1.route('/update_record_baseprice_incoterm',methods=['POST'])
@token_required
def update_record_incoterm():
        
        today = date.today()
        username=request.headers['username']
    
        now = datetime.now()
        date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
        query_parameters =json.loads(request.data)
        
        Market_Country=(query_parameters['Market_Country'])
        Customer_Group=(query_parameters["Customer_Group"])
        Incoterm1=(query_parameters["Incoterm1"])
        Product_Division =( query_parameters["Product_Division"])
        Beam_Category=(query_parameters["Beam_Category"])
        Delivering_Mill=(query_parameters["Delivering_Mill"])
        sequence_id=(query_parameters["sequence_id"])
        
        
        
        Document_Item_Currency =( query_parameters["Document_Item_Currency"])
        Amount =( query_parameters["Amount"])
        Currency =( query_parameters["Currency"])
        id_value=(query_parameters['id_value'])
        
        print("****")
        print(id_value)
        
        flag='update'
      
        tablename='SMB - Base Price - Incoterm Exceptions'     
        
        input_tuple=(tablename,id_value,sequence_id,username,Market_Country,Customer_Group,Incoterm1,Product_Division,Beam_Category,Delivering_Mill,Document_Item_Currency, Amount,Currency)
        
        col_tuple=("table_name","id","sequence_id",
              "Username",
             "Market - Country",
         "Customer Group",
         "Incoterm1", "Product Division", "Beam Category", "Delivering Mill",
         "Document Item Currency", "Amount", "Currency")
        email_status=''
        
        status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
        if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)
        
        
        
        if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
        else: return {"status":"failure"},500
   

@smb_app1.route('/add_record_baseprice_incoterm',methods=['POST'])
@token_required
def add_record_incoterm():
    
    today = date.today()
    username=request.headers['username']
    
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    Market_Country=(query_parameters['Market_Country'])
    Customer_Group=(query_parameters["Customer_Group"])
    Incoterm1=(query_parameters["Incoterm1"])
    Product_Division =( query_parameters["Product_Division"])
    Beam_Category=(query_parameters["Beam_Category"])
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
     
    flag='add'
   
    
    tablename='SMB - Base Price - Incoterm Exceptions'     
        
    input_tuple=(tablename,flag,username, Market_Country,Product_Division,Incoterm1,Customer_Group,Beam_Category,Delivering_Mill,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name","flag",  "Username",
             
              "Market - Country",
	        "Product Division",
                "Incoterm1",
		"Customer Group",
		"Beam Category",
		"Delivering Mill",
		"Document Item Currency",
		"Amount","Currency" )
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
        
    


   
@smb_app1.route('/upload_baseprice_incoterm', methods=['GET','POST'])
@token_required
def  SMB_upload_baseprice_incoterm():
    
         f=request.files['filename']
  
            
         f.save(input_directory+f.filename)
        
        
         smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
         
         df=smb_df[["id","Market - Country", "Customer Group",
    "Incoterm1", "Product Division", "Beam Category", "Delivering Mill",
    "Document Item Currency", "Amount", "Currency"]]  
         df["id"]=df["id"].astype(int)
        
         df_main = pd.read_sql('''select "id","Market - Country", "Customer Group",
    "Incoterm1", "Product Division", "Beam Category", "Delivering Mill",
    "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Base Price - Incoterm Exceptions" where "active"='1' order by sequence_id ''', con=con)
         
         df['Currency'] = df['Currency'].str.replace("'","")
         df3 = df.merge(df_main, how='left', indicator=True)
         df3=df3[df3['_merge']=='left_only']
         
         df3.columns = df3.columns.str.replace(' ', '_')
         df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
         
         df3.drop(['_merge'],axis=1,inplace=True)
         
         table=json.loads(df3.to_json(orient='records'))
         
         return {"data":table},200
       
   

@smb_app1.route('/validate_baseprice_incoterm', methods=['GET','POST'])
@token_required
def  SMB_validate_baseprice_incoterm():
    json_data=json.loads(request.data)
    username=request.headers['username']
     
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    email_status=''
    df.insert(0,'Username',username)
    
    
    
    tablename='SMB - Base Price - Incoterm Exceptions'   
    df=fun_ignore_sequence_id(df,tablename)
       
    flag='update'  
    df.insert(1,'table_name',tablename)
    col_tuple=("table_name","id","sequence_id","Username",
             "Market - Country",
	        "Product Division",
                "Incoterm1",
		"Customer Group",
		"Beam Category",
		"Delivering Mill",
		"Document Item Currency",
		"Amount","Currency")
    col_list=['table_name','id','sequence_id','Username','Market_Country','Product_Division','Incoterm1','Customer_Group','Beam_Category','Delivering_Mill','Document_Item_Currency', 'Amount', 'Currency']
    
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
  
    return {"status":"success"},200
  
        
@smb_app1.route('/download_baseprice_incoterm',methods=['GET'])

def SMB_baseprice_download_incoterm():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select "id","sequence_id","Market - Country","Customer Group",
	        
                "Incoterm1","Product Division",
		"Customer Group",
		"Beam Category",
		"Delivering Mill",
		"Document Item Currency",
		"Amount","Currency" from "SMB"."SMB - Base Price - Incoterm Exceptions" where "active"='1' order by sequence_id ''', con=con)
           
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'incoterm.xlsx'
            
            df=df.style.apply(highlight_col, axis=None)
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
           
        except:
            return {"status":"failure"},500

# *****************************************************************************************************************************************
# smb extra certificate


@smb_app1.route('/data_extra_certificate',methods=['GET','POST'])
@token_required
def extra_certificate_data():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    
    if(limit==None):
        limit=500
    if(offset==None):
        offset=0
    
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    

    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Certificate" where "active"='1' order by sequence_id  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Certificate" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app1.route('/delete__record_extra_certificate',methods=['POST','GET','DELETE'])
@token_required
def delete_extra_certificate():  
    id_value,username=json.loads(request.data)['id'],request.headers['username']
    
    
    tablename='SMB - Extra - Certificate'
   
    status=email(id_value,tablename,'delete',username=username)
    if(status=='success'):return {"status":"success"},200
    else: return {"status":"failure"},500
    
    
   


@smb_app1.route('/get_record_extra_certificate',methods=['GET','POST']) 
@token_required      
def get_record_extra_certificate():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Certificate" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app1.route('/update_record_extra_certificate',methods=['POST'])
@token_required
def add_record_extra_certificate():
    
    today = date.today()
    username=request.headers['username']
    
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Certificate=(query_parameters['Certificate'])
    Grade_Category =( query_parameters["Grade_Category"])
    Market_Country =( query_parameters["Market_Country"])
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    sequence_id=(query_parameters["sequence_id"])
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    id_value=(query_parameters['id_value'])
    flag='update'
      
    tablename='SMB - Extra - Certificate'  
    

    input_tuple=(tablename,id_value,sequence_id,username,BusinessCode,Certificate,Grade_Category,Market_Country,Delivering_Mill,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name","id","sequence_id", "Username",
             "BusinessCode",
"Certificate",
"Grade Category",
"Market - Country",

"Delivering Mill",
"Document Item Currency",
"Amount",
"Currency")      
        
    
    email_status=''
        
    status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
    if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)
    
    if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
    else: return {"status":"failure"},500
   
    
        
@smb_app1.route('/add_record_extra_certificate',methods=['GET','POST'])
@token_required
def smb_add_certificate():
    today = date.today()
    username=request.headers['username']
    
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Certificate=(query_parameters['Certificate'])
    Grade_Category =( query_parameters["Grade_Category"])
    Market_Country =( query_parameters["Market_Country"])
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
      
    flag='add'
   
    
    tablename='SMB - Extra - Certificate'
        
    input_tuple=(tablename,flag,username,BusinessCode,Certificate,Grade_Category,Market_Country,Delivering_Mill,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name","flag", "Username",
             "BusinessCode",
"Certificate",
"Grade Category",
"Market - Country",

"Delivering Mill",
"Document Item Currency",
"Amount",
"Currency")
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    
    print(status)
    print("##")
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
    
    



   
@smb_app1.route('/upload_extra_certificate', methods=['GET','POST'])
@token_required
def  Upload_extra_certificate():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[['id',"BusinessCode", "Certificate",
       "Grade Category", "Market - Country", "Delivering Mill",
       "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Certificate",
       "Grade Category", "Market - Country", "Delivering Mill",
       "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Certificate" where "active"='1' order by sequence_id ''', con=con)
            
            df['Currency'] = df['Currency'].str.replace("'","")
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500


@smb_app1.route('/validate_extra_certificate', methods=['GET','POST'])
@token_required
def  validate_extra_certificate():
    
        
    json_data=json.loads(request.data)
    username=request.headers['username']
     
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    mail_status=''
    df.insert(0,'Username',username)
    
        
    tablename='SMB - Extra - Certificate'
       
    flag='update'  
    
    df.insert(1,'table_name',tablename)
    df=fun_ignore_sequence_id(df,tablename)
    
    
    col_tuple=("table_name","id","sequence_id",
          "Username",
             "BusinessCode",
"Certificate",
"Grade Category",
"Market - Country",

"Delivering Mill",
"Document Item Currency",
"Amount",
"Currency")
    col_list=['table_name','id','sequence_id','Username','BusinessCode','Certificate','Grade_Category','Market_Country','Delivering_Mill','Document_Item_Currency', 'Amount', 'Currency']
    
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
  
    return {"status":"success"},200
         
@smb_app1.route('/download_extra_certificate',methods=['GET'])

def download_extra_certificate():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select "id",sequence_id, "BusinessCode",
"Certificate",
"Grade Category",
"Market - Country",

"Delivering Mill",
"Document Item Currency",
"Amount",
"Currency" from "SMB"."SMB - Extra - Certificate" where "active"='1' order by sequence_id ''', con=con)
          
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'extra_certificate.xlsx'
            
            df=df.style.apply(highlight_col, axis=None)
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
           
        except:
            return {"status":"failure"},500


# *****************************************************************************************************************************************
# smb extra certificate minibar



@smb_app1.route('/data_extra_certificate_minibar',methods=['GET','POST'])
@token_required
def extra_certificate_data_minibar():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    if(limit==None):
        limit=500
    if(offset==None):
        offset=0
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
   
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Certificate - MiniBar" where "active"='1' order by sequence_id  OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Certificate - MiniBar" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app1.route('/delete_record_extra_certificate_minibar',methods=['POST','GET','DELETE'])
@token_required
def delete_extra_certificate_minibar():  
   
    id_value,username=json.loads(request.data)['id'],request.headers['username']
    
    
    tablename='SMB - Extra - Certificate - MiniBar'
   
    status=email(id_value,tablename,'delete',username=username)
    if(status=='success'):return {"status":"success"},200
    else: return {"status":"failure"},500


@smb_app1.route('/get_record_extra_certificate_minibar',methods=['GET','POST'])       
def get_record_extra_certificate_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Certificate - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app1.route('/update_record_extra_certificate_minibar',methods=['POST'])
@token_required
def update_record_extra_certificate_minibar():
    
    
    today = date.today()
    username=request.headers['username']
    
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Customer_Group=(query_parameters['Customer_Group'])
   
    Market_Country =( query_parameters["Market_Country"])
    Certificate=(query_parameters['Certificate'])
    Grade_Category =( query_parameters["Grade_Category"])
    id_value=(query_parameters['id_value'])
    sequence_id=(query_parameters['sequence_id'])
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    Market_Customer=(query_parameters['Market_Customer'])
    
    flag='update'
      
    tablename='SMB - Extra - Certificate - MiniBar'  
    

    input_tuple=(tablename,id_value,sequence_id,username,BusinessCode,Certificate,Market_Country,Grade_Category,Customer_Group,Market_Customer,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name","id","sequence_id", "Username",
           "BusinessCode","Certificate","Market - Country",
"Grade Category","Customer Group","Market - Customer","Document Item Currency","Amount","Currency")      
        
    
    email_status=''
        
    status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
    if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)

    if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
    else: return {"status":"failure"},500
   
    
    
     

@smb_app1.route('/add_record_extra_certificate_minibar',methods=['GET','POST'])
@token_required
def add_record_extra_certificate_minibar():
    
    
    username=request.headers['username']
    
    
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Customer_Group=(query_parameters['Customer_Group'])
   
    Market_Country =( query_parameters["Market_Country"])
    Certificate=(query_parameters['Certificate'])
    Grade_Category =( query_parameters["Grade_Category"])
    Market_Customer=(query_parameters['Market_Customer'])
    
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
    
      
    flag='add'
   
    tablename='SMB - Extra - Certificate - MiniBar'
    
    input_tuple=(tablename,flag,username,BusinessCode,Certificate,Market_Country,Grade_Category,Customer_Group,Market_Customer,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name","flag","Username","BusinessCode","Certificate","Market - Country",
"Grade Category","Customer Group","Market - Customer","Document Item Currency","Amount","Currency")
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
    
    


    

   
@smb_app1.route('/upload_extra_certificate_minibar', methods=['GET','POST'])
@token_required
def  Upload_extra_certificate_minibar():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","BusinessCode", "Customer Group","Market - Customer",
        "Market - Country", "Certificate",
       "Grade Category", "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
            
            
            df_main = pd.read_sql('''select "id","BusinessCode", "Customer Group","Market - Customer",
       "Market - Country", "Certificate",
       "Grade Category", "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Certificate - MiniBar" where "active"='1' order by sequence_id ''', con=con)
            
            df['Currency'] = df['Currency'].str.replace("'","")
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer":"Market_Customer"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500



@smb_app1.route('/validate_extra_certificate_minibar', methods=['GET','POST'])
@token_required
def  validate_extra_certificate_minibar():
    json_data=json.loads(request.data)
    username=request.headers['username']
     
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    email_status=''
    df.insert(0,'Username',username)
    
             
    tablename='SMB - Extra - Certificate - MiniBar'
    df=fun_ignore_sequence_id(df,tablename)
    
    flag='update'  
    
    df.insert(1,'table_name',tablename)
    
    
    col_tuple=("table_name","id","sequence_id",
          "Username","BusinessCode","Certificate","Market - Country","Market - Customer",
"Grade Category","Customer Group","Document Item Currency","Amount","Currency")
    col_list=['table_name','id','sequence_id','Username','BusinessCode','Certificate','Market_Country','Market_Customer','Grade_Category','Customer_Group','Document_Item_Currency', 'Amount', 'Currency']
    
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
  
    return {"status":"success"},200
         
            
        
         
@smb_app1.route('/download_extra_certificate_minibar',methods=['GET'])

def  download_extra_certificate_minibar():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select "id",sequence_id,"BusinessCode","Customer Group","Market - Customer","Market - Country","Certificate",
"Grade Category","Document Item Currency","Amount","Currency" from "SMB"."SMB - Extra - Certificate - MiniBar" where "active"='1' order by sequence_id ''', con=con)
           
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'extra_certificate_minibar.xlsx'
            df=df.style.apply(highlight_col, axis=None)
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
           
        except:
            return {"status":"failure"},500


# *****************************************************************************************************************************************
# smb delivary mill


@smb_app1.route('/data_delivery_mill',methods=['GET','POST'])
def data_delivery_mill():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    
    
    if(limit==None):
        limit=500
    if(offset==None):
        offset=0
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
   
    
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Delivery Mill" where "active"='1' order by sequence_id OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Delivery Mill" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app1.route('/delete_record_delivery_mill',methods=['POST','GET','DELETE'])
@token_required
def delete_record_delivery_mill():  
   
    id_value,username=json.loads(request.data)['id'],request.headers['username']
    
    
    tablename='SMB - Extra - Delivery Mill'
   
    status=email(id_value,tablename,'delete',username=username)
    if(status=='success'):return {"status":"success"},200
    else: return {"status":"failure"},500



@smb_app1.route('/get_record_delivery_mill',methods=['GET','POST']) 
@token_required      
def get_record_delivery_mill():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Delivery Mill" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app1.route('/update_record_delivery_mill',methods=['POST'])
@token_required
def update_record_delivery_mill():
    
    today = date.today()
    username=request.headers['username']
    
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Market_Country=(query_parameters['Market_Country'])
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    Product_Division =( query_parameters["Product_Division"])
    Beam_Category=(query_parameters["Beam_Category"])
    
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    id_value=(query_parameters['id_value'])
    sequence_id=(query_parameters['sequence_id'])
    
    flag='update'
      
    tablename='SMB - Extra - Delivery Mill'  
    

    input_tuple=(tablename,id_value,sequence_id,username,BusinessCode,Market_Country,Delivering_Mill,Product_Division,Beam_Category,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name","id","sequence_id", "Username",
            "BusinessCode",
"Market - Country",
"Delivering Mill",
"Product Division",
"Beam Category",
"Document Item Currency",
"Amount",
"Currency")      
        
    
    email_status=''
        
    status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
    if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)
    
    if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
    else: return {"status":"failure"},500
   
    
       

@smb_app1.route('/add_record_delivery_mill',methods=['POST'])
@token_required
def add_record_delivery_mill():
    
    
    username=request.headers['username']
    
   
    query_parameters =json.loads(request.data)
    
    BusinessCode=(query_parameters['BusinessCode'])
    Market_Country=(query_parameters['Market_Country'])
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    Product_Division =( query_parameters["Product_Division"])
    Beam_Category=(query_parameters["Beam_Category"])
   
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
      
    flag='add'
   
    tablename='SMB - Extra - Delivery Mill'
    
    
    input_tuple=(tablename,flag,username,BusinessCode,Market_Country,Delivering_Mill,Product_Division,Beam_Category,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name","flag", "Username",
              "BusinessCode",
"Market - Country",
"Delivering Mill",
"Product Division",
"Beam Category",
"Document Item Currency",
"Amount",
"Currency")
    
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
    
    

        
     

   

   
@smb_app1.route('/upload_delivery_mill', methods=['GET','POST'])
@token_required
def upload_delivery_mill():
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","BusinessCode", "Market - Country",
       "Delivering Mill", "Product Division", "Beam Category",
       "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
           
            df_main = pd.read_sql('''select "id","BusinessCode", "Market - Country",
       "Delivering Mill", "Product Division", "Beam Category",
       "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Delivery Mill" where "active"='1' order by sequence_id ''', con=con)
            
            df['Currency'] = df['Currency'].str.replace("'","")
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country"},inplace=True)
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500


    
   


@smb_app1.route('/validate_delivery_mill', methods=['GET','POST'])
@token_required
def  validate_delivery_mill():
    
        
    json_data=json.loads(request.data)
    username=request.headers['username']
     
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    
    tablename='SMB - Extra - Delivery Mill'
    df=fun_ignore_sequence_id(df,tablename)
    flag='update'  
    
    df.insert(1,'table_name',tablename)
    
    
    col_tuple=("table_name","id","sequence_id",
          "Username","BusinessCode",
"Market - Country",
"Delivering Mill",
"Product Division",
"Beam Category",
"Document Item Currency",
"Amount",
"Currency")
    col_list=['table_name','id','sequence_id','Username','BusinessCode','Market_Country','Delivering_Mill','Product_Division','Beam_Category','Document_Item_Currency', 'Amount', 'Currency']
    
    
    id_value=[]
  
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
        
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
  
    return {"status":"success"},200
         
         
@smb_app1.route('/download_delivery_mill',methods=['GET'])
def  download_delivery_mill():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select "id",sequence_id, "BusinessCode",
"Market - Country",
"Delivering Mill",
"Product Division",
"Beam Category",
"Document Item Currency",
"Amount",
"Currency" from "SMB"."SMB - Extra - Delivery Mill" where "active"='1' order by sequence_id ''', con=con)
           
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'delivery_mill.xlsx'
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
           
        except:
            return {"status":"failure"},500
    
    

# *****************************************************************************************************************************************
# smb delivary mill minibar




@smb_app1.route('/data_delivery_mill_minibar',methods=['GET','POST'])
@token_required
def data_delivery_mill_minibar():
    # query_paramters 
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    
    
    if(limit==None):
        limit=500
    if(offset==None):
        offset=0
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    # fetching the data from database and filtering    
    try:
        df = pd.read_sql('''select * from "SMB"."SMB - Extra - Delivery Mill - MiniBar" where "active"='1' order by sequence_id OFFSET {} LIMIT {}'''.format(lowerLimit,upperLimit), con=con)
        count=db.query('select count(*) from "SMB"."SMB - Extra - Delivery Mill - MiniBar" where "active"=1 ')[0][0]
        df.columns = df.columns.str.replace(' ', '_')
        
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)
        
        if(search_string!="all" and search_string!=None):
                      df=df[df.eq(search_string).any(1)]
        
        table=json.loads(df.to_json(orient='records'))
        
        return {"data":table,"totalCount":count},200         
    except:
        return {"statuscode":500,"msg":"failure"},500
        

        

  
@smb_app1.route('/delete_record_delivery_mill_minibar',methods=['POST','GET','DELETE'])
@token_required
def delete_record_delivery_mill_minibar():  
   
    id_value,username=json.loads(request.data)['id'],request.headers['username']
    
    
    tablename='SMB - Extra - Delivery Mill - MiniBar'
   
    status=email(id_value,tablename,'delete',username=username)
    if(status=='success'):return {"status":"success"},200
    else: return {"status":"failure"},500




@smb_app1.route('/get_record_delivery_mill_minibar',methods=['GET','POST'])       
def get_record_delivery_mill_minibar():
    id_value=request.args.get('id')  
    
    query='''SELECT * FROM "SMB"."SMB - Extra - Delivery Mill - MiniBar" where id={}'''.format(id_value)
    
    try:
        df = pd.read_sql(query, con=con)
        df.columns = df.columns.str.replace(' ', '_')
        df.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer"},inplace=True)
        record=json.loads(df.to_json(orient='records'))
        return {"record":record},200
    except:
        return {"status":"failure"},500
    
        


@smb_app1.route('/update_record_delivery_mill_minibar',methods=['POST'])
@token_required
def update_record_delivery_mill_minibar():

    today = date.today()
    username=request.headers['username']
    
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    query_parameters =json.loads(request.data)
    
    
    Market_Country=(query_parameters['Market_Country'])
    Market_Customer_Group=(query_parameters['Market_Customer_Group'])
   
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    Product_Division =( query_parameters["Product_Division"])
    id_value=(query_parameters['id_value'])
    sequence_id=(query_parameters['sequence_id'])
    Market_Customer=(query_parameters["Market_Customer"])
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])

    flag='update'
      
    tablename='SMB - Extra - Delivery Mill - MiniBar'  
    

    input_tuple=(tablename,id_value,sequence_id,username,Market_Country,Market_Customer_Group,Market_Customer,Delivering_Mill,Product_Division,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name","id","sequence_id", "Username",
            "Market - Country",
       "Market - Customer Group", "Market - Customer", "Delivering Mill",
       "Product Division", "Document Item Currency", "Amount", "Currency")      
        
    
    email_status=''
        
    status=upsert(col_tuple,input_tuple,flag,tablename,id_value)
    if(status['status']=='success'):email_status=email([status['tableid']],tablename,username=username)
    if(email_status=='success' or status['status']=='move_direct'): return {"status":"success"},200
    else: return {"status":"failure"},500
   
    
       

@smb_app1.route('/add_record_delivery_mill_minibar',methods=['POST'])
@token_required
def add_record_delivery_mill_minibar():
    
    username=request.headers['username']
    
    
    query_parameters =json.loads(request.data)
    
    
    Market_Country=(query_parameters['Market_Country'])
    Market_Customer_Group=(query_parameters['Market_Customer_Group'])
    Market_Customer=(query_parameters["Market_Customer"])
    Delivering_Mill=(query_parameters["Delivering_Mill"])
    Product_Division =( query_parameters["Product_Division"])
    Document_Item_Currency =( query_parameters["Document_Item_Currency"])
    Amount =( query_parameters["Amount"])
    Currency =( query_parameters["Currency"])
    
      
    flag='add'
      
    tablename='SMB - Extra - Delivery Mill - MiniBar'  
    
    input_tuple=(tablename,flag,username,Market_Country,Market_Customer_Group,Market_Customer,Delivering_Mill,Product_Division,Document_Item_Currency,Amount,Currency)
    col_tuple=("table_name","flag", "Username",
            "Market - Country",
       "Market - Customer Group","Market - Customer",  "Delivering Mill",
       "Product Division", "Document Item Currency", "Amount", "Currency")
    
    
    status=upsert(col_tuple,input_tuple,flag,tablename)
    if(status['status']=='success'): email_status=email([status['tableid']],tablename,username=username)
    
    if(email_status=='success'): return {"status":"success"},200
    else: return {"status":"failure"},500
    
    
    
    
   
@smb_app1.route('/upload_delivery_mill_minibar', methods=['GET','POST'])
@token_required
def upload_delivery_mill_minibar():
    
        f=request.files['filename']
  
            
        f.save(input_directory+f.filename)
        
        try:
            smb_df=pd.read_excel(input_directory+f.filename,dtype=str)
            
            df=smb_df[["id","Market - Country",
       "Market - Customer Group", "Market - Customer", "Delivering Mill",
       "Product Division", "Document Item Currency", "Amount", "Currency"]]  
            df["id"]=df["id"].astype(int)
           
            
            df_main = pd.read_sql('''select "id","Market - Country",
       "Market - Customer Group","Market - Customer", "Delivering Mill",
       "Product Division", "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Delivery Mill - MiniBar" where "active"='1' order by sequence_id ''', con=con)
            
            df['Currency'] = df['Currency'].str.replace("'","")
            
            df3 = df.merge(df_main, how='left', indicator=True)
            df3=df3[df3['_merge']=='left_only']
            
            df3.columns = df3.columns.str.replace(' ', '_')
            df3.rename(columns={"Market_-_Country":"Market_Country","Market_-_Customer_Group":"Market_Customer_Group","Market_-_Customer":"Market_Customer","Zip_Code_(Dest)":"Zip_Code_Dest"},inplace=True)  
                
            
            df3.drop(['_merge'],axis=1,inplace=True)
            
            table=json.loads(df3.to_json(orient='records'))
            
            return {"data":table},200
        except:
            return {"status":"failure"},500


@smb_app1.route('/validate_delivery_mill_minibar', methods=['GET','POST'])
@token_required
def  validate_delivery_mill_minibar():
    
        
    json_data=json.loads(request.data)
    username=request.headers['username']
     
    now = datetime.now()
    date_time= now.strftime("%m/%d/%Y, %H:%M:%S")
    
    df=pd.DataFrame(json_data["billet"]) 
    
    df.insert(0,'Username',username)
    df.insert(1,'date_time',date_time)
    
    tablename='SMB - Extra - Delivery Mill - MiniBar'
    df=fun_ignore_sequence_id(df,tablename)
    flag='update'  
   
    df.insert(1,'table_name',tablename)
   
   
    col_tuple=("table_name","id","sequence_id",
          "Username","Market - Country",
       "Market - Customer Group","Market - Customer", "Delivering Mill",
       "Product Division", "Document Item Currency", "Amount", "Currency")
    col_list=['table_name','id','sequence_id','Username','Market_Country','Market_Customer_Group','Market_Customer','Delivering_Mill','Product_Division','Document_Item_Currency', 'Amount', 'Currency']
   
   
    id_value=[]
 
    df=df[ col_list]
    
    for i in range(0,len(df)):
        status=upsert(col_tuple,tuple(df.loc[i]),flag,tablename,df['id'][i])
        id_value.append(status['tableid'])
       
    if(status['status']=='success'):
        email_status=email(id_value,tablename,username=username)
 
    return {"status":"success"},200

@smb_app1.route('/download_delivery_mill_minibar',methods=['GET'])
def  download_delivery_mill_minibar():
   
        now = datetime.now()
        try:
            df = pd.read_sql('''select "id",sequence_id,"Market - Country",
       "Market - Customer Group","Market - Customer",  "Delivering Mill",
       "Product Division", "Document Item Currency", "Amount", "Currency" from "SMB"."SMB - Extra - Delivery Mill - MiniBar" where "active"='1' order by sequence_id ''', con=con)
            
            t=now.strftime("%d-%m-%Y-%H-%M-%S")
            file=download_path+t+'delivery_mill_minibar.xlsx'
            
            df=df.style.apply(highlight_col, axis=None)
            print(file)
            df.to_excel(file,index=False)
            
            return send_file(file, as_attachment=True)
           
        except:
            return {"status":"failure"},500
    
        
        

    
   


