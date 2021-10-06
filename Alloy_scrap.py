
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
import pandas as pd
from pathlib import Path
import os
from sqlalchemy import create_engine
import getpass
from datetime import datetime
from datetime import date

app = Flask(__name__)
CORS(app)
con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')
cur = con.cursor()
output_directory='C:/Users/Administrator/Documents/Output/'
Non_processed="C:/Users/Administrator/Documents/Non_processed"
Processed_directory='C:/Users/Administrator/Documents/Processed_files'
folder_path="C:/Users/Administrator/Documents/Input_files"
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



@app.route('/Alloy_wire_upload',methods=['GET','POST'])
def upload_files():
 
        f =request.files["filename"]
        table_1=[{}]
        now = datetime.now()
        today = date.today()
        
        
        columns=['Month/Year', 'Mill', 'Customer ', 'Customer ID', 'Internal Grade',
       'Sales Grade (Optional)', 'Monthly Alloy Surcharge',
       'Monthly Alloy Surcharge BARS', 'Monthly Alloy Surcharge SEMIS ',
       'Monthly Alloy Surcharge PEELED ', 'Internal Grade Gandrange',
       'Exception of Gandarange delivering mill (Monthly, Average of last 3 months, or Trimester Caluclation)',
       'Key', 'Duplicate', 'FakturaVerkettung Du\n']
        
        f.save('C:/Users/Administrator/Documents/Input_files/'+f.filename)
        stock_df = pd.read_excel(folder_path +"/"+f.filename)
        stock_df_columns=list(stock_df.columns)
        try:
            if(columns==stock_df_columns):
                print("inside")
                VKORG="0300"
                DST_CH="02"
                DIV="02"
                COND_TYPE="Z133"
                
                data1=stock_df[['Month/Year', 'Monthly Alloy Surcharge','Customer ID','Internal Grade']]
                
                data1.rename(columns={'Month/Year': 'Month_year', 'Monthly Alloy Surcharge': 'Amount','Customer ID':'Customer_ID','Internal Grade':'Internal_Grade'}, inplace=True)
                
                # data1= data1["Month_year"].dt.strftime("%y%m")

                data1.insert(0,'VKORG',str(VKORG))
                data1.insert(1,'COND_TYPE',str(COND_TYPE))
                data1.insert(2,'DST_CH',str(DST_CH))
                data1.insert(3,'DIV',str(DIV))
                
                pending_wire=data1[data1.isna().any(axis=1)]
                
                Path("C:/Users/Administrator/Documents/Non_processed").mkdir(parents=True, exist_ok=True)
                pending_wire.to_csv(Non_processed+'/'+f.filename+'.csv')
                  
                
                data1=data1.dropna()
                data1['Month_year']=data1['Month_year'].astype(str)
                data1["Month_year"] =data1["Month_year"].str.replace("_", "")
    
                data1=data1[(pd.to_datetime(data1['Month_year'], format='%Y%m')) >=today.strftime('%Y-%m') ]
                
                
                table_1 = data1.to_json(orient='records')
                status="success"
                return  json.dumps({"data":table_1,"filename":f.filename})
                
            else:
                 status='incorrect file format'
                 os.remove('C:/Users/Administrator/Documents/Input_files/'+f.filename)
                 os.remove("C:/Users/Administrator/Documents/Non_processed/"+f.filename)
                 return  {"statuscode":"500","message":"incorrect"}
        except:
              status='incorrect file format'
              return  {"statuscode":"500","message":"incorrect"}
       
 
@app.route('/Alloy_billet_upload',methods=['GET','POST'])
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
        
        
        f.save('C:/Users/Administrator/Documents/Input_files/'+f.filename)
        stock_df = pd.read_excel(folder_path +"/"+f.filename)
        stock_df_columns=list(stock_df.columns)
        
        try:
            if(columns==stock_df_columns):
                print("inside")
                VKORG="0200"
                DST_CH="05"
                DIV="04"
                COND_TYPE="ZLEZ"
                
                data2=stock_df[['Monat', 'LZ','WARENEMPFAENGER_NR','Materialnr','dRUCKSPERRE']]
                
                data2.rename(columns={'Monat': 'Month_year', 'LZ': 'Amount'}, inplace=True)
                # data= data["Month_year"].dt.strftime("%y%m")
                
                data2.insert(0,'VKORG',VKORG)
                data2.insert(1,'COND_TYPE',COND_TYPE)
                data2.insert(2,'DST_CH',DST_CH)
                data2.insert(3,'DIV',DIV)
                
                
                data2['Month_year']=data2['Month_year'].astype(str)
                data2["Month_year"] =data2["Month_year"].str.replace("_", "")
                
                data2=data2[(pd.to_datetime(data2['Month_year'], format='%Y%m')) >=today.strftime('%Y-%m')]
               
                
                pending_billet=data2[data2.isna().any(axis=1)]
                Path("C:/Users/Administrator/Documents/Non_processed").mkdir(parents=True, exist_ok=True)
                pending_billet.to_csv(Non_processed+'/'+f.filename+'.csv')
                  
                
                data2=data2.dropna()
                
                
                # data=data.fillna(0)
                table_2 = data2.to_json(orient='records')
                status="success"
                return  json.dumps({"data":table_2,"filename":f.filename})
                
                
            else:
                 status='incorrect file format'
                 os.remove('C:/Users/Administrator/Documents/Input_files/'+f.filename)
                 os.remove("C:/Users/Administrator/Documents/Non_processed/"+f.filename)
                 return  {"statuscode":"500","message":"incorrect"}
        except:
              return  {"statuscode":"500","message":"incorrect"}
        



@app.route('/scrap_upload',methods=['GET','POST'])
def upload_files_scrap ():
 
        f =request.files['filename']
        table_3=[{}]
        now = datetime.now()
        today = date.today()
        VKORG="0200"
        
        
        columns=['Month_Year', 'Model', 'Amount']
        
        f.save('C:/Users/Administrator/Documents/Input_files/'+f.filename)
        stock_df = pd.read_excel(folder_path +"/"+f.filename)
        stock_df_columns=list(stock_df.columns)
        try:
            if(columns==stock_df_columns):
                print("inside")
                VKORG="0200"
                DST_CH="05"
                DIV="04"
                COND_TYPE="ZSCZ"
                
                data3=stock_df[['Month_Year', 'Model','Amount']]
                
                data3.rename(columns={'Month_Year': 'Month_year'}, inplace=True)
                
                # data1= data1["Month_year"].dt.strftime("%y%m")

                data3.insert(0,'VKORG',str(VKORG))
                data3.insert(1,'COND_TYPE',str(COND_TYPE))
                data3.insert(2,'DST_CH',str(DST_CH))
                data3.insert(3,'DIV',str(DIV))
                
                pending_scrap=data3[data3.isna().any(axis=1)]
                
                Path("C:/Users/Administrator/Documents/Non_processed").mkdir(parents=True, exist_ok=True)
                pending_scrap.to_csv(Non_processed+'/'+f.filename+'.csv')
                    
                
                
                data3=data3.dropna()
                data3['Month_year']=data3['Month_year'].astype(str)
                data3["Month_year"] =data3["Month_year"].str.replace("_", "")
                
                data3=data3[(pd.to_datetime(data3['Month_year'], format='%Y%m')) >=today.strftime('%Y-%m') ]
                
                
                table_3 = data3.to_json(orient='records')
                status="success"
                return  json.dumps({"data":table_3,"filename":f.filename})
                
            else:
                 status='incorrect file format'
                 os.remove('C:/Users/Administrator/Documents/Input_files/'+f.filename)
                 os.remove("C:/Users/Administrator/Documents/Non_processed/"+f.filename)
                 return  {"statuscode":"500","message":"incorrect"}
        except:
               return  {"statuscode":"500","message":"incorrect"}
        


@app.route('/Alloy_wire_validate',methods=['GET','POST'])
def validate_files1():
    
    username = getpass.getuser()
    now = datetime.now()
    today = date.today()
    
    
    query_parameters = json.loads(request.data)
    filename=(query_parameters["filename"])
    wire =( query_parameters["wire"])
    
    wire_df=pd.DataFrame(wire)
    
    
    
    
    try:
        
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
        wire_df.to_sql('alloy_surcharge_wire',con=engine, schema='alloy_surcharge',if_exists='append', index=False)
        
        
        date_time= today.strftime("%Y%m%d")
        counter=str(Batch_ID)
        cond_type="Z133"
        sales_org="0300"
        
        # Path("C:\ocpphase1\ftp\Q72").mkdir(parents=True, exist_ok=True)
        wire_df.to_csv("C:/ocpphase1/ftp/Q72/"+date_time+counter+'_'+cond_type+'_'+sales_org+'.csv')
    
        
        return {"message":"success"}
    except:
       return  {"statuscode":"500","message":"incorrect"}
       
    




@app.route('/Alloy_billet_validate',methods=['GET','POST'])
def validate_files2():
    
    username = getpass.getuser()
    now = datetime.now()
    today = date.today()
    
    
    query_parameters = json.loads(request.data)
    filename=(query_parameters["filename"])
    billet =( query_parameters["billet"])
    billet_df=pd.DataFrame(billet)
    
    
    
    
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
        
        # Path("C:\ocpphase1\ftp\Q72").mkdir(parents=True, exist_ok=True)
        billet_df.to_csv("C:/ocpphase1/ftp/Q72/"+date_time+counter+'_'+cond_type+'_'+sales_org+'.csv')
    
        
        status='success'
        return {"message":"success"}
        
        
    except:
        return  {"statuscode":"500","message":"incorrect"}
       


@app.route('/Alloy_scrap_validate',methods=['GET','POST'])
def validate_files3():
    
    
    username = getpass.getuser()
    now = datetime.now()
    today = date.today()
    
    query_parameters = json.loads(request.data)
    filename=(query_parameters["filename"])
    scrap =( query_parameters["scrap"])
    scrap_df=pd.DataFrame(scrap)
    
    
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
        
        # Path("C:\ocpphase1\ftp\Q72").mkdir(parents=True, exist_ok=True)
        scrap_df.to_csv("C:/ocpphase1/ftp/Q72/"+date_time+counter+'_'+cond_type+'_'+sales_org+'.csv')
        
        
        
        
        status='success'
        return {"message":"success"}
    except:
        return  {"statuscode":"500","message":"incorrect"}
       










@app.route('/alloy_wire_search',methods=['GET','POST'])
def search1():
    query_parameters = json.loads(request.data)
    wire=query_parameters["wire"]
    search_string=query_parameters['search_string']
    
    wire_df=pd.DataFrame(wire)
    
    try:
      data=wire_df[wire_df.eq(search_string).any(1)] 
      
      
      if(search_string==""):
          table_1=wire_df.to_json(orient='records')
          return{"data":table_1}
      
      else:
          if(len(data)==0):
              table_1=[{}]
          else:
              table_1=data.to_json(orient='records')
          return {"data":table_1}
      
    except:
        return  {"statuscode":"500","message":"incorrect"}





@app.route('/alloy_billet_search',methods=['GET','POST'])
def search2():
    query_parameters = json.loads(request.data)
    billet=query_parameters["scrap"]
    search_string=query_parameters['search_string']
    
    billet_df=pd.DataFrame(billet)
    
    try:
      data=billet_df[billet_df.eq(search_string).any(1)] 
      if(search_string==""):
          table_2=billet_df.to_json(orient='records')
          return{"data":table_2}
      
      else:
          if(len(data)==0):
              table_2=[{}]
          else:
              table_2=data.to_json(orient='records')
          return {"data":table_2}
    except:
        return  {"statuscode":"500","message":"incorrect"}






  
@app.route('/alloy_scrap_search',methods=['GET','POST'])
def search3():
    query_parameters = json.loads(request.data)
    scrap=query_parameters["scrap"]
    search_string=query_parameters['search_string']
    
    scrap_df=pd.DataFrame(scrap)
    
    try:
      data=scrap_df[scrap_df.eq(search_string).any(1)] 
      if(search_string==""):
          table_3=scrap_df.to_json(orient='records')
          return{"data":table_3}
      
      else:
          if(len(data)==0):
              table_3=[{}]
          else:
              table_3=data.to_json(orient='records')
          return {"data":table_3}
    except:
        return  {"statuscode":"500","message":"incorrect"}
    

    
    

@app.route('/alloy_surcharge_history',methods=['GET','POST'])
def history():
    
    
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    search_string=request.args.get("search_string")
    try:
        search_string=int(search_string)
    except:
        search_string=str(search_string)
        
    
    print(search_string)
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    
    
    
    try:
        cur.execute('rollback')
        query='''select distinct "Batch_ID", "Username","date_time","filename" ,"COND_TYPE" from  alloy_surcharge.alloy_surcharge_billet
    union 
    select distinct "Batch_ID", "Username","date_time","filename","COND_TYPE" from  alloy_surcharge.alloy_surcharge_wire
    union
    select distinct  "Batch_ID","Username","date_time","filename","COND_TYPE" from  alloy_surcharge.scrap_surcharge_billet
    order by date_time  desc OFFSET {} LIMIT {} '''.format(lowerLimit,upperLimit)
    
        cur.execute(query)
        history_data=cur.fetchall()
        columns=['Batch_ID','username','date_time','filename','condition_type']
        df=pd.DataFrame(history_data,columns=columns)
        
        if(search_string !=None and search_string !="all"):
            filtered_data=df[df.eq(search_string).any(1)] 
            filtered_data=json.loads(filtered_data.to_json(orient='records'))
        else:
            filtered_data=json.loads(df.to_json(orient='records'))
            
        
        
        return {"data":filtered_data}
    except:
        return {"statuscode":"500","message":"failed"}



    


@app.route('/getfile_data',methods=['GET','POST'])
def getfiles():
    

    query_parameters = json.loads(request.data)
    
    filename=query_parameters["filename"]
    Batch_ID=query_parameters["Batch_ID"]
    condition_type=query_parameters['condition_type']
    
    
    try:
        if(condition_type=="Z133"):
            query='''select "VKORG","DIV","DST_CH","COND_TYPE","Month_year","Internal_Grade","Customer_ID" from alloy_surcharge.alloy_surcharge_wire where "filename"= '{}' and "Batch_ID"='{}'   '''.format(filename,Batch_ID)
            
            cur.execute(query)
            data=cur.fetchall()
            columns=["VKORG","DIV","DST_CH","COND_TYPE","Month_year","Internal_Grade","Customer_ID"]
            df=pd.DataFrame(data,columns=columns)
            table_wire=json.loads(df.to_json(orient='records'))
            return {"data":table_wire}
        
        if(condition_type=="ZLEZ"):
            query='''select "VKORG","DIV","DST_CH","COND_TYPE","Month_year","Amount","dRUCKSPERRE","Materialnr","WARENEMPFAENGER_NR" from alloy_surcharge.alloy_surcharge_billet where "filename"= '{}' and "Batch_ID"='{}'   '''.format(filename,Batch_ID)
            
            cur.execute(query)
            data=cur.fetchall()
            columns=["VKORG","DIV","DST_CH","COND_TYPE","Month_year","Amount","dRUCKSPERRE","Materialnr","WARENEMPFAENGER_NR"]
            df=pd.DataFrame(data,columns=columns)
            table_billet=json.loads(df.to_json(orient='records'))
            return {"data":table_billet}
        
        
        if(condition_type=="ZSCZ"):
            query='''select "VKORG","DIV","DST_CH","COND_TYPE","Month_year","Amount","Model" from alloy_surcharge.scrap_surcharge_billet where "filename"= '{}' and "Batch_ID"='{}'   '''.format(filename,Batch_ID)
            
            cur.execute(query)
            data=cur.fetchall()
            columns=["VKORG","DIV","DST_CH","COND_TYPE","Month_year","Amount","Model"]
            df=pd.DataFrame(data,columns=columns)
            table_scrap=json.loads(df.to_json(orient='records'))
            return {"data":table_scrap}  
        
    
    
    except:
        
         return {"statuscode":"500","message":"failed"}
    
 
  



@app.route('/search_files',methods=['GET','POST'])
def search_files():
    query_parameters = json.loads(request.data)
    
    table=query_parameters["table"]
    
    search_string=query_parameters['search_string']
    
    table_df=pd.DataFrame(table)
    
    try:
      data=table_df[table_df.eq(search_string).any(1)] 
      if(search_string==""):
          data=table_df.to_json(orient='records')
          return{"data":data}
      
      else:
          if(len(data)==0):
              data=[{}]
          else:
              data=data.to_json(orient='records')
          return {"data":data}
    except:
        return  {"statuscode":"500","message":"incorrect"}
    































@app.route('/Alloy_scrap', methods=['GET','POST'])
def  Alloy_scrap():
    
    
        table_1=[{"DFS":"SDF","DFS":"SDF"},{"DFS":"SDF","DFS":"SDF"}]
        table_2=[{"DFS":"SDF","DFS":"SDF"},{"DFS":"SDF","DFS":"SDF"}]
        table_3=[{"DFS":"SDF","DFS":"SDF"},{"DFS":"SDF","DFS":"SDF"}]
        table_1=json.dumps(table_1)
        table_2=json.dumps(table_2)
        table_3=json.dumps(table_3)
        username = getpass.getuser()
        
        
        
        query_parameters = json.loads(request.data)
        
        
        inputaction=query_parameters['inputaction']
        
        engine = create_engine('postgresql://postgres:ocpphase01@ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com:5432/offertool')
        folder_path="C:/Users/Administrator/Documents/Input_files"
        
        files = [ file.split('.')[0] for file in os.listdir(folder_path) if  file.endswith( ('.xlsx') ) ]

        now = datetime.now()
        today = date.today()

        dict_1={'0300':['02','02'],'0200':['05','04']}
        
        
        for i in files:
            stock_df = pd.read_excel(folder_path +"/"+i+'.xlsx')    
            
            list_1=i.split('_')
            l=dict_1[list_1[-1]]
            VKORG=list_1[-1]
            DST_CH=l[0]
            DIV=l[1]
            COND_TYPE=list_1[-2]
            
            
            if(list_1[-2]=='ZLEZ'):
                
                data=stock_df[['Monat', 'LZ','WARENEMPFAENGER_NR','Materialnr','dRUCKSPERRE']]
                
                data.rename(columns={'Monat': 'Month_year', 'LZ': 'Amount'}, inplace=True)
                # data= data["Month_year"].dt.strftime("%y%m")
                
                data.insert(0,'VKORG',VKORG)
                data.insert(1,'COND_TYPE',COND_TYPE)
                data.insert(2,'DST_CH',DST_CH)
                data.insert(3,'DIV',DIV)
                
                
                data['Month_year']=data['Month_year'].astype(str)
                data["Month_year"] =data["Month_year"].str.replace("_", "")
                
                data=data[(pd.to_datetime(data['Month_year'], format='%Y%m')) >=today.strftime('%Y-%m')]
               
                pending_billet=data[data[['Amount','WARENEMPFAENGER_NR','Materialnr','dRUCKSPERRE']].isnull()]
                pending_billet=data[data.isna().any(axis=1)]
                
                data=data.dropna()
                
                
                # data=data.fillna(0)
                table_1 = data.to_json(orient='records')
                
                
                
                
                if(inputaction=='validated_billet'):
                      
                      # data= data.astype({'VKORG': 'str', 'DIV': 'str','DST_CH':'str'})
                      # data[['VKORG','COND_TYPE','DST_CH','DIV']] =  data[['VKORG','COND_TYPE','DST_CH','DIV']].astype(str)
                      
                      Path("C:/Users/Administrator/Documents/Output").mkdir(parents=True, exist_ok=True)
                      data.to_csv(output_directory+i+'.csv')
                      
                      Path("C:/Users/Administrator/Documents/Non_processed").mkdir(parents=True, exist_ok=True)
                      pending_billet.to_csv(Non_processed+'/'+i+'.csv')
                    
                      shutil.move(folder_path +"/"+i+'.xlsx', Processed_directory)
                      
                      
                     
                      cur.execute('rollback')
                      cur.execute('select max("Batch_ID") from alloy_surcharge.alloy_surcharge_billet;')
                      max_id=cur.fetchall()
                     
                      if(max_id[0][0] == None):
                             Batch_ID=1
                      else:
                             Batch_ID=((max_id[0][0])+1)
                             
                             
                           
                      dt_string = now.strftime("%d/%m/%Y %H:%M:%S") 
                      data.insert(0,'Batch_ID',Batch_ID)
                      data.insert(1,'Username',username)
                      data.insert(2,'date_time',dt_string)
                      
                      data.to_sql('alloy_surcharge_billet',con=engine, schema='alloy_surcharge',if_exists='append', index=False)
                      
                      
                      
                
               
            if(list_1[-2]=='Z133'):
                
                data1=stock_df[['Month/Year', 'Monthly Alloy Surcharge','Customer ID','Internal Grade']]
                
                data1.rename(columns={'Month/Year': 'Month_year', 'Monthly Alloy Surcharge': 'Amount','Customer ID':'Customer_ID','Internal Grade':'Internal_Grade'}, inplace=True)
                
                # data1= data1["Month_year"].dt.strftime("%y%m")

                data1.insert(0,'VKORG',str(VKORG))
                data1.insert(1,'COND_TYPE',str(COND_TYPE))
                data1.insert(2,'DST_CH',str(DST_CH))
                data1.insert(3,'DIV',str(DIV))
                
                
                
                pending_wire=data1[data1.isna().any(axis=1)]
                
                data1=data1.dropna()
                data1['Month_year']=data1['Month_year'].astype(str)
                data1["Month_year"] =data1["Month_year"].str.replace("_", "")
                
                
                
                data1=data1[(pd.to_datetime(data1['Month_year'], format='%Y%m')) >=today.strftime('%Y-%m') ]
                
                
                
                
                
                
                table_2 = data1.to_json(orient='records')
                
                if(inputaction=='validated_wire'):
                    data1[['VKORG','COND_TYPE','DST_CH','DIV']] =  data1[['VKORG','COND_TYPE','DST_CH','DIV']].astype(str)
                      
                    Path("C:/Users/Administrator/Documents/Output").mkdir(parents=True, exist_ok=True)
                    data1.to_csv(output_directory+i+'.csv')
                    
                    Path("C:/Users/Administrator/Documents/Non_processed").mkdir(parents=True, exist_ok=True)
                    pending_wire.to_csv(Non_processed+'/'+i+'.csv')
                    
                    
                    
                    shutil.move(folder_path +"/"+i+'.xlsx', Processed_directory)
                    
                    
                    cur.execute('rollback')
                    cur.execute('select max("Batch_ID") from alloy_surcharge.alloy_surcharge_wire;')
                    max_id=cur.fetchall()
                     
                    if(max_id[0][0] == None):
                            Batch_ID=1
                    else:
                            Batch_ID=((max_id[0][0])+1)
                           
                    dt_string = now.strftime("%d/%m/%Y %H:%M:%S") 
                    data1.insert(0,'Batch_ID',Batch_ID)
                    data1.insert(1,'Username',username)
                    data1.insert(2,'date_time',dt_string)
                    
                    data1.to_sql('alloy_surcharge_wire',con=engine, schema='alloy_surcharge',if_exists='append', index=False)
  
            if(list_1[-2]=='ZSCZ'):
                
                data3=stock_df[['Month_Year', 'Model','Amount']]
                
                data3.rename(columns={'Month_Year': 'Month_year'}, inplace=True)
                
                # data1= data1["Month_year"].dt.strftime("%y%m")

                data3.insert(0,'VKORG',str(VKORG))
                data3.insert(1,'COND_TYPE',str(COND_TYPE))
                data3.insert(2,'DST_CH',str(DST_CH))
                data3.insert(3,'DIV',str(DIV))
                
                pending_scrap=data3[data3.isna().any(axis=1)]
                
                
                data3=data3.dropna()
                data3['Month_year']=data3['Month_year'].astype(str)
                data3["Month_year"] =data3["Month_year"].str.replace("_", "")
                
                data1=data3[(pd.to_datetime(data3['Month_year'], format='%Y%m')) >=today.strftime('%Y-%m') ]
                
                
                
                
                table_3 = data3.to_json(orient='records')
                
                if(inputaction=='validated_scrap'):
                    data3[['VKORG','COND_TYPE','DST_CH','DIV']] =  data3[['VKORG','COND_TYPE','DST_CH','DIV']].astype(str)
                      
                    Path("C:/Users/Administrator/Documents/Output").mkdir(parents=True, exist_ok=True)
                    data3.to_csv(output_directory+i+'.csv')
                    
                    
                    Path("C:/Users/Administrator/Documents/Non_processed").mkdir(parents=True, exist_ok=True)
                    pending_scrap.to_csv(Non_processed+'/'+i+'.csv')
                    
                    shutil.move(folder_path +"/"+i+'.xlsx', Processed_directory)
                    
                    Batch_ID=1
                
                           
                    dt_string = now.strftime("%d/%m/%Y %H:%M:%S") 
                    data3.insert(0,'Batch_ID',Batch_ID)
                    data3.insert(1,'Username',username)
                    data3.insert(2,'date_time',dt_string)
                    
                    data3.to_sql('scrap_surcharge_billlet',con=engine, schema='alloy_surcharge',if_exists='append', index=False)
     
                
        
        return ({'billet':table_1,'wire':table_2,'scrap':table_3})
     
     

if __name__ == '__main__':
    app.run()