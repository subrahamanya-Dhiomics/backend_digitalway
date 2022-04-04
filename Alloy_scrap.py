
"""
Created on Thu Oct  12 07:33:38 2021

@author: subbu

"""

from flask import Blueprint,current_app

import numpy as np
import pandas as pd
import traceback
import time
import json
from flask import Flask, request, render_template
from flask import jsonify
from flask_cors import CORS
from json import JSONEncoder
from collections import OrderedDict
from flask import Blueprint
import calendar
import psycopg2
import shutil
from pathlib import Path
import os
from sqlalchemy import create_engine
import getpass
from datetime import datetime
from datetime import date
import random
import pysftp
from base64 import decodebytes
import paramiko
import pymssql
 
scrap_app = Blueprint('scrap_app', __name__)

CORS(scrap_app)

con = psycopg2.connect(dbname='offertool',user='pgadmin',password='Sahara_17',host='offertool2-qa.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')
cur = con.cursor()




consql = psycopg2.connect(dbname='offertool',user='pgadmin',password='Sahara_17',host='offertool2-qa.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')



# csv_out_path="C:/Users/Administrator/Documents/"
# input_path="C:/Users/Administrator/Documents/"

# csv_out_path="/home/ubuntu/mega_dir/"
# input_path="/home/ubuntu/mega_dir/"

engine = create_engine('postgresql://postgres:ocpphase01@ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com:5432/offertool')


class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)


def tupleToList(tupleVar):
            listVar=[]
            for tupleVarEach in tupleVar:
                listVar.append(tupleVarEach[0])
            return(listVar)

# Hostname = "3.65.20.36"
Hostname="3.65.20.36"
Username = "sftp_user"
Password = "sftp_user@321"

def funUpload(fileToUpload,filename):
    print("Initiate SFTP Upload")
    try:
        # keydata = b"""AAAAB3NzaC1yc2EAAAADAQAB..."""
        # key = paramiko.RSAKey(data=decodebytes(keydata))
        cnopts = pysftp.CnOpts()
        # cnopts.hostkeys.add('example.com', 'ssh-rsa', key)
        cnopts.hostkeys=None
        with pysftp.Connection(host=Hostname, username=Username, password=Password, cnopts=cnopts) as sftp:
            print("Connection succesfully stablished ... ")
        
            # # Switch to a remote directory
            # sftp.cwd('/sftp_user')
        
            localFilePath=fileToUpload
            # Define the remote path where the file will be uploaded
            remoteFilePath = '/sftp_user/'+filename
            
            sftp.put(localFilePath, remoteFilePath)
            
            # Switch to a remote directory
            sftp.cwd('/sftp_user')
        
            # Obtain structure of the remote directory '/var/www/vhosts'
            directory_structure = sftp.listdir_attr()
        
            # Print data
            for attr in directory_structure:
                print(attr.filename, attr)
    except Exception as ex:
        print(ex)
    
    
    print("Upload Operation Done")



@scrap_app.route('/Alloy_wire_upload',methods=['GET','POST'])
def upload_files():
    
        data01=pd.DataFrame()
        ds=''
        data02=pd.DataFrame()
       
        f =request.files["filename"]
        table_1=[{}]
        today = date.today()
        f.save(input_path+f.filename)
     
        
        stock_df = pd.read_excel(input_path +f.filename)
         
            # filtering only hamburg and duisburg data
        stock_df=stock_df[(stock_df['Mill']=='Duisburg') | (stock_df['Mill']=='Hamburg')]
        
        # selecting and renaming the columns
        data1=stock_df[['Month/Year', 'Monthly Alloy Surcharge','Customer ID','Internal Grade','Mill']]
        data1=data1.rename(columns={"Month/Year": "Month_year", "Monthly Alloy Surcharge": "Amount","Customer ID":"Customer_ID","Internal Grade":"Internal_Grade"})
    
        # adding required columns with constant values
        VKORG="0300"
        DST_CH="02"
        DIV="02"
        COND_TYPE="Z133"
        data1.insert(0,'VKORG',str(VKORG))
        data1.insert(1,'COND_TYPE',str(COND_TYPE))
        data1.insert(2,'DST_CH',str(DST_CH))
        data1.insert(3,'DIV',str(DIV))
        
        data1['VKORG'][data1.Mill == "Hamburg"] ="0400"
        data1['DST_CH'][data1.Mill == "Hamburg"] ="07"
        
        
        # filling the zeros & dropping the null values
        data1=data1.dropna()
        data1['Internal_Grade']=data1["Internal_Grade"].astype(str)
        
        
        data1['Internal_Grade']=data1['Internal_Grade'].apply(lambda x: x.zfill(4))
       
        # filtering only current month and next month data
        
        data1['Month_year']=data1['Month_year'].astype(str)
        data1["Month_year"] =data1["Month_year"].str.replace("_", "")
        data1['Month_year'] = data1['Month_year'].str.split('.').str[0]
        
        # data1=data1[(pd.to_datetime(data1['Month_year'], format='%Y%m')) == '2022-02' ]
        
        
        
        
        df=pd.read_sql('select * from offertool.ordertoofferV2',con=consql)
        
        
        data1['Customer_ID']=data1['Customer_ID'].astype(str,errors='ignore')
        data1['Internal_Grade']=data1['Internal_Grade'].astype(str)
        df['grade']=df['grade'].astype(str)
        df['accountcode']=df['accountcode'].astype(str)
        
        
     
        df['frompriceperiod'] = pd.to_datetime(df['frompriceperiod'])
        df['topriceperiod'] = pd.to_datetime(df['topriceperiod'])
        
        columns=['OFFER_ALLOY','OFFER_EFF_PR','OFFER_ALLOY_NUMBER','OFFER_ EFF_PR _NUMBER']
        
        n=[9,10,11,12]
        for i in range(0,len(columns)):
          data1.insert(n[i],columns[i],'')
    
        
        for mon in range(2):
            
            if(mon==0):   
               ds=''
               ds=data1[(pd.to_datetime(data1['Month_year'], format='%Y%m')) == today.strftime('%Y-%m')]
            if(mon==1):
                ds=''
                ds=data1[(pd.to_datetime(data1['Month_year'], format='%Y%m')) > today.strftime('%Y-%m')]
        
        
            data=df.merge(ds,left_on=['accountcode','grade'],right_on=['Customer_ID','Internal_Grade'])
            
            
            data=data[   ((pd.to_datetime(data['Month_year'], format='%Y%m')) >= (pd.to_datetime(data['frompriceperiod'], format='%Y%m'))  )  &  ((pd.to_datetime(data['Month_year'], format='%Y%m')) <=(pd.to_datetime(data['topriceperiod'], format='%Y%m')))]
            
            data=data[['accountcode','pricemodel','offerid','Month_year','grade','frompriceperiod','topriceperiod']]
           
            data['pricemodel']=data['pricemodel'].astype(float,errors='ignore')
            data['pricemodel']=data['pricemodel'].astype(int,errors='ignore')
            data['pricemodel'] = data['pricemodel']. replace(np.nan, None)
            data['offerid']=data['offerid'].astype(int,errors='ignore')
           
            
            a=[]
            
            
            for i in list(data.index):
                   customer_id=data['accountcode'][i]
                   internal_grade=data['grade'][i]
                   pricemodel=data['pricemodel'][i]
                   offerid=data['offerid'][i]
            
                   for ind in list(ds.index):
                    
                     if (ds['Customer_ID'][ind]==customer_id and ds['Internal_Grade'][ind]==internal_grade):
                        if(data['pricemodel'][i]  in (2,4,5,7,None)):
                           
                           if(offerid not in a):
                              print(ind)
                              if(ds['OFFER_ALLOY'][ind]==''):
                                ds['OFFER_ALLOY'][ind]= 1
                                ds['OFFER_ALLOY_NUMBER'][ind]= ds['OFFER_ALLOY_NUMBER'][ind]+str(offerid)
                              else:
                                ds['OFFER_ALLOY'][ind]= ds['OFFER_ALLOY'][ind]+1
                                ds['OFFER_ALLOY_NUMBER'][ind]= ds['OFFER_ALLOY_NUMBER'][ind]+'_'+str(offerid)   
                              a.append(offerid)
                    
                        if(data['pricemodel'][i] in (1,3,6,8)):
                           if(offerid not in a):
                            print(ind)
                            
                            if(ds['OFFER_ EFF_PR _NUMBER'][ind]==''):
                             ds['OFFER_ EFF_PR _NUMBER'][ind]= ds['OFFER_ EFF_PR _NUMBER'][ind]+str(offerid)
                             ds['OFFER_EFF_PR'][ind]= 1
                            else:
                               ds['OFFER_ EFF_PR _NUMBER'][ind]= ds['OFFER_ EFF_PR _NUMBER'][ind]+'_'+str(offerid)
                               ds['OFFER_EFF_PR'][ind]= ds['OFFER_EFF_PR'][ind]+1
                            a.append(offerid)
            if(mon==0):
                data01=ds
            if(mon==1):
                data02=ds
                
      
         
        d = pd.concat([data01,data02],    # Combine vertically
                          ignore_index = True,
                          sort = False)
        d=d.replace('', 0)
        table_1 = d.to_json(orient='records')
        return {"data":table_1,"filename":f.filename},200
                        
        
             
        
        
                      
        
        
        
            
        
        
              






            
        
       

               
@scrap_app.route('/Alloy_wire_validate',methods=['GET','POST'])
def validate_files1():
    
    username = getpass.getuser()
    now = datetime.now()
    today = date.today()
    
    
    query_parameters = json.loads(request.data)
    filename=(query_parameters["filename"])
    wire =( query_parameters["wire"])
    
    
    wire_df=pd.DataFrame(wire)
    wire_df.rename(columns={'Monthly_Deviation':'Monthly Deviation'})
    out_df=wire_df.copy()
    wire_df.drop(['Mill'],axis=1,inplace=True)
    
    cur.execute('rollback')
    cur.execute('select max("Batch_ID") from alloy_surcharge.alloy_surcharge_wire;')
    max_id=cur.fetchall()
    if(max_id[0][0] == None):
            Batch_ID=1
    else:
            Batch_ID=((max_id[0][0])+1)      
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    wire_df.insert(0,'filename',filename)
    wire_df.insert(0,'Batch_ID',Batch_ID)
    wire_df.insert(1,'Username',username)
    wire_df.insert(2,'date_time',dt_string)
    
    wire_df.to_csv('wire_df.csv')
    wire_df.to_sql('alloy_surcharge_wire',con=engine, schema='alloy_surcharge',if_exists='append', index=False)
    
    date_time= today.strftime("%Y%m%d")
    counter=str(Batch_ID)
    cond_type="Z133"
    sales_org={"Duisburg":"0300","Hamburg":"0400"}
    
    for i in sales_org:
      if(i=="Duisburg"):
          filename=date_time+counter+'_'+cond_type+'_'+sales_org["Duisburg"]+'.csv'
          file_path=csv_out_path+filename
          df1=out_df[out_df['Mill']=='Duisburg']
          df1.drop(['Mill'],axis=1,inplace=True)
          
          df1.to_csv(file_path, index = False)
          funUpload(file_path,filename)
      else:
          filename=date_time+counter+'_'+cond_type+'_'+sales_org["Hamburg"]+'.csv'
          file_path=csv_out_path+filename
          df2=out_df[out_df['Mill']=='Hamburg']
          df2.drop(['Mill'],axis=1,inplace=True)
          df2.to_csv(file_path, index = False)
          print("hello")
          funUpload(file_path,filename)
    
    
    print("Done File: Upload to SFTP")
    
    return {"message":"success"},200
   

     
         
       
            

       
        


     
     
     
@scrap_app.route('/Alloy_billet_upload',methods=['GET','POST'])
def upload_files_billet ():
 
        f =request.files["filename"]
        table_2=[{}]
        now = datetime.now()
        today = date.today()
        
        
        columns=['WARENEMPFAENGER_NR', 'WARENEMPFAENGER', 'BESTELLBEZ_KUNDE',
       'KUNDENNORM', 'KN_DAT', 'Materialnr', 'lz_elemente', 'ianr',
       'SEL_NR_MELDUNG', 'dRUCKSPERRE', 'si_min_kunde', 'Sivon', 'Sibis',
       'SI_MAX_KUNDE', 'MN_MIN_KUNDE', 'Mnvon', 'Mnbis', 'MN_MAX_KUNDE',
       'CR_MIN_KUNDE', 'Crvon', 'crbis', 'CR_MAX_KUNDE', 'MO_MIN_KUNDE',
       'Movon', 'Mobis', 'MO_MAX_KUNDE', 'NI_MIN_KUNDE', 'Nivon', 'Nibis',
       'NI_MAX_KUNDE', 'V_MIN_KUNDE', 'Vvon', 'Vbis', 'V_MAX_KUNDE', 'LZ',
       'elemente', 'Monat', 'id']
        
        
        f.save(input_path+f.filename)
        stock_df = pd.read_excel(input_path +f.filename)
        stock_df_columns=list(stock_df.columns)
        
        try:
            
                
                VKORG="0200"
                DST_CH="05"
                DIV="04"
                COND_TYPE="ZLEZ"
                
                data2=stock_df[['Monat', 'LZ','WARENEMPFAENGER_NR','SEL_NR_MELDUNG','dRUCKSPERRE']]
                data2.rename(columns={'Monat': 'Month_year', 'LZ': 'Amount'}, inplace=True)
               
                data2.insert(0,'VKORG',VKORG)
                data2.insert(1,'COND_TYPE',COND_TYPE)
                data2.insert(2,'DST_CH',DST_CH)
                data2.insert(3,'DIV',DIV)
                
                
                data2['Month_year']=data2['Month_year'].astype(str)
                data2["Month_year"] =data2["Month_year"].str.replace("_", "")
                
                data2=data2[(pd.to_datetime(data2['Month_year'], format='%Y%m')) >=today.strftime('%Y-%m')]
               
                
                data2=data2.dropna()
                
                
                # data=data.fillna(0)
                table_2 = data2.to_json(orient='records')
                status="success"
                return  json.dumps({"data":table_2,"filename":f.filename})
                
                
         
        except:
              return  {"statuscode":"500","message":"incorrect"},500
        



@scrap_app.route('/scrap_upload',methods=['GET','POST'])
def upload_files_scrap ():
 
        f =request.files['filename']
        table_3=[{}]
        now = datetime.now()
        today = date.today()
        VKORG="0200"
         
        
        try:
        
        
            f.save(input_path+f.filename)
            data = pd.read_excel(input_path +f.filename,sheet_name="Shuffled SS")
            
            df=data[((data['Model']=='Former') & (data['Product']=='All') & (data['Division'].isnull())) | ((data['Model']=='Market') & (data['Product']=='Rolled Billets') & (data['Division']=='RCS')) | ((data['Model']=='New ') & (data['Product']=='Wire Rod') & (data['Division']=='WR')) ]
    
            
                
            
            VKORG="0200"
            DST_CH="05"
            DIV="04"
            COND_TYPE="ZSCZ"
            
            data3=df[['Month','Model','Product','Value','Monthly Deviation']]
            
            data3.rename(columns={'Month': 'Month_year','Value':'Amount','Monthly Deviation':'Monthly_Deviation'}, inplace=True)
            
            
            # data1= data1["Month_year"].dt.strftime("%y%m")
    
            data3.insert(0,'VKORG',str(VKORG))
            data3.insert(1,'COND_TYPE',str(COND_TYPE))
            data3.insert(2,'DST_CH',str(DST_CH))
            data3.insert(3,'DIV',str(DIV))
            
            
            data3['Month_year']=data3['Month_year'].astype(str)
            data3["Month_year"] =data3["Month_year"].str.replace("_", "")
            
            data3=data3[(pd.to_datetime(data3['Month_year'], format='%Y%m')) >=today.strftime('%Y-%m') ]
            
            data3.loc[(df.Model =='Former'), 'Model'] = 'O'
            data3.loc[(df.Model =='New '), 'Model'] = 'N'
            data3.loc[(df.Model =='Market'), 'Model'] = 'M'
            
            table_3 =data3.to_json(orient='records')
            status="success"
            return  {"data":table_3,"filename":f.filename},200
            
        except:
            return  json.dumps({"data":table_3,"filename":f.filename})
 

@scrap_app.route('/Alloy_billet_validate',methods=['GET','POST'])
def validate_files2():
    
    username = getpass.getuser()
    now = datetime.now()
    today = date.today()
    
    
    query_parameters = json.loads(request.data)
    filename=(query_parameters["filename"])
    billet =( query_parameters["billet"])
    billet_df=pd.DataFrame(billet)
    out_df=billet_df.copy()
    
    
    
    
    try:
        
        cur.execute('rollback')
        cur.execute('select max("Batch_ID") from alloy_surcharge.alloy_surcharge_billet;')
        max_id=cur.fetchall()
        if(max_id[0][0] == None):
                Batch_ID=1
        else:
                Batch_ID=((max_id[0][0])+1)      
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        billet_df.insert(0,'filename',filename)
        billet_df.insert(0,'Batch_ID',Batch_ID)
        billet_df.insert(1,'Username',username)
        billet_df.insert(2,'date_time',dt_string)
        billet_df.to_sql('alloy_surcharge_billet',con=engine, schema='alloy_surcharge',if_exists='append', index=False)
        
        date_time= today.strftime("%Y%m%d")
        counter=str(Batch_ID)
        cond_type="ZLEZ"
        sales_org="0200"
        
        filename=date_time+counter+'_'+cond_type+'_'+sales_org+'.csv'
        file_path=csv_out_path+filename
        
        # Path("C:\ocpphase1\ftp\Q72").mkdir(parents=True, exist_ok=True)
        # out_df.reset_index(drop=True, inplace=True)
        out_df.to_csv(file_path, index = False)
        funUpload(file_path, filename)
        
        status='success'
        return {"message":"success"},200
        
        
    except:
        return  {"statuscode":"500","message":"incorrect"},500
       


@scrap_app.route('/Alloy_scrap_validate',methods=['GET','POST'])
def validate_files3():
    
    
    username = getpass.getuser()
    now = datetime.now()
    today = date.today()
    
    query_parameters = json.loads(request.data)
    filename=(query_parameters["filename"])
    scrap =( query_parameters["scrap"])
    scrap_df=pd.DataFrame(scrap)
    out_df=scrap_df.copy()
    out_df=out_df.drop(['Product','Monthly_Deviation'], axis = 1)
  
    
    
    
    try:
        cur.execute('rollback')
        cur.execute('select max("Batch_ID") from alloy_surcharge.scrap_surcharge_billet;')
        max_id=cur.fetchall()
        if(max_id[0][0] == None):
                Batch_ID=1
        else:
                Batch_ID=((max_id[0][0])+1)  

        
            
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        scrap_df.insert(0,'filename',filename)
        scrap_df.insert(0,'Batch_ID',Batch_ID)
        scrap_df.insert(1,'Username',username)
        scrap_df.insert(2,'date_time',dt_string)
        scrap_df.to_sql('scrap_surcharge_billet',con=engine, schema='alloy_surcharge',if_exists='append', index=False)
        
        
        date_time= today.strftime("%Y%m%d")
        counter=str(Batch_ID)
        cond_type="ZSCZ"
        sales_org="0200"
        
        filename=date_time+counter+'_'+cond_type+'_'+sales_org+'.csv'
        file_path=csv_out_path+filename
        
        # Path("C:\ocpphase1\ftp\Q72").mkdir(parents=True, exist_ok=True)
        # out_df.reset_index(drop=True, inplace=True)
        out_df.to_csv(file_path, index = False)
        funUpload(file_path,filename)
    
    
        
        status='success'
        return {"message":"success"},200
    except:
        return  {"statuscode":"500","message":"incorrect"},500
       









@scrap_app.route('/alloy_surcharge_history',methods=['GET','POST'])
def history():
    
    
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    print(limit)
    print(offset)
    
    search_string=request.args.get("search_string")
    
    
    
    
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    
    
    try:
        cur.execute('rollback')
        
        query1='''select distinct "Batch_ID", "Username","date_time","filename" ,"COND_TYPE" from  alloy_surcharge.alloy_surcharge_billet
    union 
    select distinct "Batch_ID", "Username","date_time","filename","COND_TYPE" from  alloy_surcharge.alloy_surcharge_wire
    union
    select distinct  "Batch_ID","Username","date_time","filename","COND_TYPE" from  alloy_surcharge.scrap_surcharge_billet
    order by date_time  desc'''
    
        cur.execute(query1)
        total=cur.fetchall()
        df1=pd.DataFrame(total)
        
        
        
        query='''select distinct "Batch_ID", "Username","date_time","filename" ,"COND_TYPE" from  alloy_surcharge.alloy_surcharge_billet
    union 
    select distinct "Batch_ID", "Username","date_time","filename","COND_TYPE" from  alloy_surcharge.alloy_surcharge_wire
    union
    select distinct  "Batch_ID","Username","date_time","filename","COND_TYPE" from  alloy_surcharge.scrap_surcharge_billet
    order by date_time desc OFFSET {} LIMIT {} '''.format(lowerLimit,upperLimit)
    
        cur.execute(query)
        history_data=cur.fetchall()
        columns=['Batch_ID','username','date_time','filename','condition_type']
        df=pd.DataFrame(history_data,columns=columns)
        print(df)
        
        if(search_string !=None and search_string !="all"):
            
            filtered_data=df[df.eq(search_string).any(1)] 
            
            filtered_data=json.loads(filtered_data.to_json(orient='records'))
        else:
            filtered_data=json.loads(df.to_json(orient='records'))
        
        
        
        
        return {"data":filtered_data,"totalCount":len(df1)},200
    except:
        return {"statuscode":"500","message":"failed"},500



    


@scrap_app.route('/getfile_data',methods=['GET','POST'])
def getfiles():
    

    
    
    filename=request.args.get("filename")
    Batch_ID=request.args.get("Batch_ID",type=int)
    condition_type=request.args.get("condition_type")
    
    
    try:
        
        if(condition_type=="Z133"):
            query='''select "VKORG","DIV","DST_CH","COND_TYPE","Month_year","Internal_Grade","Customer_ID" from alloy_surcharge.alloy_surcharge_wire where "filename"= '{}' and "Batch_ID"='{}'   '''.format(filename,Batch_ID)
            
            cur.execute(query)
            data=cur.fetchall()
            columns=["VKORG","DIV","DST_CH","COND_TYPE","Month_year","Internal_Grade","Customer_ID"]
            df=pd.DataFrame(data,columns=columns)
            table_wire=json.loads(df.to_json(orient='records'))
            return {"table_wire":table_wire},200
        
        if(condition_type=="ZLEZ"):
            query='''select "VKORG","DIV","DST_CH","COND_TYPE","Month_year","Amount","dRUCKSPERRE","SEL_NR_MELDUNG","WARENEMPFAENGER_NR" from alloy_surcharge.alloy_surcharge_billet where "filename"= '{}' and "Batch_ID"='{}'   '''.format(filename,Batch_ID)
            
            cur.execute(query)
            data=cur.fetchall()
            columns=["VKORG","DIV","DST_CH","COND_TYPE","Month_year","Amount","dRUCKSPERRE","Materialnr","WARENEMPFAENGER_NR"]
            df=pd.DataFrame(data,columns=columns)
            table_billet=json.loads(df.to_json(orient='records'))
            return {"table_billet":table_billet},200
        
        
        if(condition_type=="ZSCZ"):
            query='''select "VKORG","DIV","DST_CH","COND_TYPE","Month_year","Amount","Model" from alloy_surcharge.scrap_surcharge_billet where "filename"= '{}' and "Batch_ID"='{}'   '''.format(filename,Batch_ID)
            
            cur.execute(query)
            data=cur.fetchall()
            columns=["VKORG","DIV","DST_CH","COND_TYPE","Month_year","Amount","Model"]
            df=pd.DataFrame(data,columns=columns)
            table_scrap=json.loads(df.to_json(orient='records'))
            return {"table_scrap":table_scrap} ,200 
        else:
            return {"statuscode":"500","message":"failed"},500
            
    
    
    except:
        
         return {"statuscode":"500","message":"failed"},500
