# -*- coding: utf-8 -*-
"""
Created on Tue Nov  9 11:42:38 2021

@author: subbu
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
import json
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


taskbar1 = Blueprint('taskbar1', __name__)

CORS(taskbar1)
con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')
cur = con.cursor()
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
            print('inside insert')
            var = 'failed'
            try:
                self.cursor = self.connection.cursor()
#                print("HEY")
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
@taskbar1.route('/taskbar1_data', methods=['POST','GET'])
def add_income():
    
    try:
        search_string=request.args.get('search_string')
        
        customer=request.args.get('customer',type=str)
        pending_with=request.args.get('pending_with')
        status='Pending Business Validation'
        created=request.args.get('created')
        
        offerid=request.args.get('offerid')
        cust_ref=request.args.get('customer_ref')
    except:
        return {"satatus":"failure"}
    
    
    
   
    
    wherestr=''
    flag=0
    
    if(customer!='All' and customer!='all'  and customer!=None):
       
        if(flag==0):wherestr+="where C.ACCOUNTNAME = '{}' ".format(customer)
        else:wherestr+=" and  C.ACCOUNTNAME = '{}' ".format(customer)
        flag=1
    if(pending_with!='All' and pending_with!='all' and pending_with!=None ):
       
        if(flag==0):wherestr+=" where C.pgl_validator = '{}' ".format(pending_with)
        else:wherestr+=" and  C.pgl_validator ='{}' ".format(pending_with)
        flag=1
        
    if(status!='All' and status!='all' and status!=None):
        
        if(flag==0):wherestr+="where S.DESCRIPTION = '{}' ".format(status)
        else:wherestr+=" and  S.DESCRIPTION = '{}' ".format(status)
        flag=1
    
    if(created!='All' and created!='all' and created!=None ):
        
        created=created.replace(created.split(' ')[-1],'').strip()+'+00'
        if(flag==0):wherestr+="where P.CREATIONDATETIME = '{}' ".format(created)
        else:wherestr+=" and  P.CREATIONDATETIME = '{}' ".format(created)
        flag=1
        print(created)
        print("************************************")
    
    if(offerid !='All' and offerid !='all' and offerid != None ):
        
        if(flag==0):wherestr+='where  P.OFFERID = {}'.format(offerid)
        else:wherestr+=' and  P.OFFERID = {}'.format(offerid)
        flag=1
    
    if(cust_ref !='All'  and cust_ref !='all'  and cust_ref != None ):
        
        if(flag==0):wherestr+="where P.RFQREFERENCE = '{}' ".format(cust_ref)
        else:wherestr+=" and  P.RFQREFERENCE = '{}' ".format(cust_ref)
        flag=1
    
    
    
   
    
    query1='''SELECT DISTINCT P.OFFERID,
	LPAD(P.OFFERID::text,
		6,
		'0') OFFERIDFULL, --P.name,
    PL.DESCRIPTION PLANTTEXT,
	
	C.ACCOUNTNAME,
	P.INCOTERMCODE,
    p.pgl_validation_levels,
    pgl_validator,
	P.STARTDATE,
	P.CREATIONDATETIME,
	P.CLOSEDATE,
	P.PRICEPERIODFROM,
	P.PRICEPERIODTO,
	P.OFFERSTATUSCODE,
	P.RFQREFERENCE,
	S.DESCRIPTION OFFERSTATUSTEXT,
	
	EC.NAME CREATIONUSER,
	D.DESCRIPTION DIVISION,
	COALESCE(P.TOTALQUANTITY,
		0) TONS,
	COALESCE(P.TOTALITEMS,
		0) ITEMS,
	COALESCE(P.TOTALSUBITEMS,
		0) SUBLITEMS
FROM OFFERTOOL.OFFER P


INNER JOIN OFFERTOOL.ACCOUNT C ON C.ACCOUNTCODE = P.ACCOUNTCODE
LEFT JOIN OFFERTOOL.OFFERSTATUS S ON S.OFFERSTATUSCODE = P.OFFERSTATUSCODE
LEFT JOIN OFFERTOOL.DIVISION D ON D.DIVISIONCODE = P.DIVISIONCODE
LEFT JOIN OFFERTOOL.EMPLOYEE EC ON EC.EMPLOYEENUMBER = P.CREATIONEMPLOYEENUMBER
LEFT JOIN OFFERTOOL.PLANT PL ON PL.PLANTCODE = P.PLANTCODE {} '''.format(wherestr)
    print(query1)
    
    try:
        
       
        db.insert('rollback')
        df = pd.read_sql(query1, con=con)
        
        if(search_string!="All" and search_string!='all' and search_string!=None):
                          df=df[df.eq(search_string).any(1)]
                          
        df['creationdatetime']=df['creationdatetime'].astype(str)
        df['closedate']=df['closedate'].astype(str)
                          
        created=list(set(df.creationdatetime))
        
        
        df['creationdatetime'] = df['creationdatetime'].str.split(' ').str[0]
       
        df['creationdatetime']=pd.to_datetime(df['creationdatetime'], format='%Y/%m/%d')
        df['closedate'] = df['closedate'].str.split(' ').str[0]
       
        df['closedate']=pd.to_datetime(df['closedate'], format='%Y/%m/%d')
        
        
        df['creationdatetime']=df['creationdatetime'].astype(str)
        df['closedate']=df['closedate'].astype(str)
            
        data=json.loads(df.to_json(orient='records'))
        
        customer_name=list(set(df.accountname))
        customer_name.append('All')
        # status=list(set(df.pgl_validation_levels))
   
        status=list(set(df.offerstatustext))
        status.append('All')
        created.append('All')
        pending_with=[]
        
        pending_with.append('All')
        
        
        return jsonify({"data":data ,"customer_name":customer_name,"status":status,"pending_with":pending_with,"created":created}),200
    except:
        return {"status":"failure"},500

@taskbar1.route('/invoice_payments', methods=['POST','GET'])
def  invoice_payments():
    customer=request.args.get('customer')
    invoice_aging=request.args.get('invoice_ageing')
    invoice_posting_date=request.args.get('invoice_posting_date')
    
    search_string=request.args.get("search_string")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    
    try:
        lowerLimit=offset*limit 
        upperLimit=lowerLimit+limit
    except:
        pass
    
    wherestr=''
    flag=0
    
    
    if(customer!='All' and customer!='all'  and customer!=None):
       
        if(flag==0):wherestr+="where n.NAME1 = '{}' ".format(customer)
        else:wherestr+=" and  n.NAME1 = '{}' ".format(customer)
        flag=1
    if(invoice_aging!='All' and invoice_aging!='all' and invoice_aging!=None ):
       
        if(flag==0):wherestr+=" where C.pgl_validator = '{}' ".format(invoice_aging)
        else:wherestr+=" and  C.pgl_validator ='{}' ".format(invoice_aging)
        flag=1
        
    if(invoice_posting_date!='All' and invoice_posting_date!='all' and invoice_posting_date!=None):
        
        if(flag==0):wherestr+="where b.BUDAT = '{}' ".format(invoice_posting_date)
        else:wherestr+=" and  b.BUDAT = '{}' ".format(invoice_posting_date)
        flag=1
        
    query='''SELECT b.KUNNR Customer_Number,n.NAME1 Customer_Name,b.BELNR Sales_Order_Number, b.VBEL2 Invoice_Number,
b.BUDAT Invoice_Posting_date,b.BUZEI Item_Number,b.WRBTR Amount,extract(day from CURRENT_DATE-(ZFBDT::date+((concat(ZBD1T::varchar,' day'))::varchar)::INTERVAL)) Invoice_Aging,
CONCAT((CASE WHEN floor(extract(day from CURRENT_DATE-(ZFBDT::date+((concat(ZBD1T::varchar,' day'))::varchar)::INTERVAL))/30) <= 0 THEN 0 ELSE (floor(extract(day from CURRENT_DATE-(ZFBDT::date+((concat(ZBD1T::varchar,' day'))::varchar)::INTERVAL))/30)*30)+1 END)::VARCHAR,
'-',(CASE WHEN ceil(extract(day from CURRENT_DATE-(ZFBDT::date+((concat(ZBD1T::varchar,' day'))::varchar)::INTERVAL))/30) <= 0 THEN 0 ELSE (ceil(extract(day from CURRENT_DATE-(ZFBDT::date+((concat(ZBD1T::varchar,' day'))::varchar)::INTERVAL))/30)*30) END)::VARCHAR) Invoice_Aging_Bucket

FROM invoice.BSID b INNER JOIN invoice.KNA1 n ON n.KUNNR=b.KUNNR  {}  ; '''.format(wherestr)

    print(query)
    try:
        db.insert('rollback')
        df = pd.read_sql(query, con=con)
        customer_name=list(set(df.customer_name))
        customer_name.append('All')
       
        
        df['invoice_posting_date']=df['invoice_posting_date'].astype(str)
        df['invoice_posting_date'] = df['invoice_posting_date'].str.split(' ').str[0]  
        df['invoice_posting_date']=pd.to_datetime(df['invoice_posting_date'], format='%Y/%m/%d')
        df['invoice_posting_date']=df['invoice_posting_date'].astype(str)
        
        
        invoice_posting_date=list(set(df.invoice_posting_date))
        invoice_posting_date.append('All')
        if(search_string!="All" and search_string!='all' and search_string!=None):
                              df=df[df.eq(search_string).any(1)]
        df=df.loc[lowerLimit:upperLimit]
        data=json.loads(df.to_json(orient='records'))
        
    
        
        
        return jsonify({"data":data,"customer_name":customer_name,"invoice_posting_date":invoice_posting_date})
    except:
        return {"stataus":"sucess"},500