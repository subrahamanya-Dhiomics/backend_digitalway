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
    
    
    
    search_string=request.args.get('search_string')
    
    customer=request.args.get('customer')
    pending_with=request.args.get('pending_with')
    status=request.args.get('status')
    created=request.args.get('created')
    offerid=request.args.get('offerid')
    cust_ref=request.args.get('cust_ref')
    
   
    
    wherestr=''
    flag=0
    
    if(customer!='all' and customer!=None):
       
        if(flag==0):wherestr+='where C.ACCOUNTNAME = {} '.format(customer)
        else:wherestr+=' and  C.ACCOUNTNAME ={}'.format(customer)
        flag=1
    if(pending_with!='all' and pending_with!=None ):
       
        if(flag==0):wherestr+='where C.pgl_validator = {}'.format(pending_with)
        else:wherestr+=' and  C.pgl_validator ={}'.format(pending_with)
        flag=1
        
    if(status!='all' and status!=None):
        
        if(flag==0):wherestr+='where p.pgl_validation_levels = {}'.format(status)
        else:wherestr+=' and  p.pgl_validation_levels = {}'.format(status)
        flag=1
    
    if(created!='all' and created!=None ):
        
        if(flag==0):wherestr+='where P.CREATIONDATETIME = {}'.format(pending_with)
        else:wherestr+=' and  P.CREATIONDATETIME = {}'.format(pending_with)
        flag=1
    
    if(offerid !='all' and offerid != None ):
        
        if(flag==0):wherestr+='where  P.OFFERID = {}'.format(offerid)
        else:wherestr+=' and  P.OFFERID = {}'.format(offerid)
        flag=1
    
    
   
    
    query='''SELECT DISTINCT P.OFFERID,
	LPAD(P.OFFERID::text,
		6,
		'0') OFFERIDFULL, --P.name,
    PL.DESCRIPTION PLANTTEXT,
	CO.DESCRIPTION COUNTRYTEXT,
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
	COALESCE(E.NAME,

		EC.NAME) LASTMODIFICATIONUSER,
	EC.NAME CREATIONUSER,
	D.DESCRIPTION DIVISION,
	COALESCE(P.TOTALQUANTITY,
		0) TONS,
	COALESCE(P.TOTALITEMS,
		0) ITEMS,
	COALESCE(P.TOTALSUBITEMS,
		0) SUBLITEMS
FROM OFFERTOOL.OFFER P
--INNER JOIN OFFERTOOL.MYACCOUNT M ON P.ACCOUNTCODE = M.ACCOUNTCODE
INNER JOIN OFFERTOOL.MYEMPLOYEE ME ON P.SALESRESPONSIBLECODE = ME.EMPLOYEENUMBER
INNER JOIN OFFERTOOL.ACCOUNT C ON C.ACCOUNTCODE = P.ACCOUNTCODE
LEFT JOIN OFFERTOOL.OFFERSTATUS S ON S.OFFERSTATUSCODE = P.OFFERSTATUSCODE
LEFT JOIN OFFERTOOL.EMPLOYEE E ON E.EMPLOYEENUMBER = P.MODIFICATIONEMPLOYEENUMBER
LEFT JOIN OFFERTOOL.DIVISION D ON D.DIVISIONCODE = P.DIVISIONCODE
LEFT JOIN OFFERTOOL.EMPLOYEE EC ON EC.EMPLOYEENUMBER = P.CREATIONEMPLOYEENUMBER
LEFT JOIN OFFERTOOL.PLANT PL ON PL.PLANTCODE = P.PLANTCODE
LEFT JOIN OFFERTOOL.COUNTRY CO ON CO.COUNTRYCODE = P.COUNTRYCODE {} '''.format(wherestr)

    
        
    df = pd.read_sql(query, con=con)
    
    data=json.loads(df.to_json(orient='records'))
    
    customer_name=list(set(df.accountname))
    # status=list(set(df.pgl_validation_levels))
    df['creationdatetime']=pd.to_datetime(df['creationdatetime'])
    
    
    df['creationdatetime']=df['creationdatetime'].astype(str)
    created=list(set(df.creationdatetime))
    pending_with=['testing','testing1','testing_2','testing_3']
    
    return {"data":data ,"customer_name":customer_name,"pending_with":pending_with,"created":created},200

        

