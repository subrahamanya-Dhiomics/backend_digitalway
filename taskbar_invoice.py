# -*- coding: utf-8 -*-
"""
Created on Thu Dec 23 17:39:09 2021

@author: subbu
"""
from flask import Blueprint
import pandas as pd

import json
from flask import Flask, request, send_file, render_template, make_response
from flask import jsonify
from flask_cors import CORS

import psycopg2

from sqlalchemy import create_engine

from smb_phase2 import token_required

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
            var = ''
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

taskbar_invoice_app = Blueprint('taskbar_invoice_app', __name__)

CORS(taskbar_invoice_app)

con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')



cursor=con.cursor()


  
@taskbar_invoice_app.route('/invoice_payments',methods=['GET','POST'])
@token_required
def invoice():
    
    customer=request.args.get('customer')
    
    invoice_posting_date_from=request.args.get('invoice_posting_date_from')
    invoice_posting_date_to=request.args.get('invoice_posting_date_to')
    
    invoice_aging_from=request.args.get('invoice_ageing_from')
    invoice_aging_to=request.args.get('invoice_ageing_to')
    search_string=request.args.get("search_string")
    invoice_aging_bucket=request.args.get("invoice_ageing_bucket")
    
    limit=request.args.get("limit",type=int)
    offset=request.args.get("offset",type=int)
    
    # pagination logic
    lowerLimit=offset*limit 
    upperLimit=lowerLimit+limit
    wherestr=''
    flag=0
    
    
    if(customer!='All' and customer!='all'  and customer!=None):
       
        if(flag==0):wherestr+="where tf.Customer_Name = '{}' ".format(customer)
        else:wherestr+=" and  tf.Customer_Name = '{}' ".format(customer)
        flag=1
        
    if(invoice_aging_to!='All' and invoice_aging_to!='all' and invoice_aging_to!=None ):
        
       
        if(flag==0):wherestr+=" where tf.Invoice_Aging  between '{}' and '{}' ".format(invoice_aging_from,invoice_aging_to)
        else:wherestr+=" and  tf.Invoice_Aging between '{}' and '{}' ".format(invoice_aging_from,invoice_aging_to)
        flag=1
        
    if(invoice_posting_date_to!='All' and invoice_posting_date_to!='all' and invoice_posting_date_to!=None):
        
        if(flag==0):wherestr+="where tf.Invoice_Posting_Date between '{}'  and '{}' ".format(invoice_posting_date_from,invoice_posting_date_to)
        else:wherestr+=" and  tf.Invoice_Posting_Date between '{}'  and '{}' ".format(invoice_posting_date_from,invoice_posting_date_to)
        flag=1
        
        
    if(invoice_aging_bucket!='All' and invoice_aging_bucket!='all' and invoice_aging_bucket!=None ):
           try:
               
               bucket=invoice_aging_bucket.split(' ')[0]
               bucket_from=bucket.split('-')[0]
               bucket_to=bucket.split('-')[1]
           except:
               pass
       
           if(flag==0):
           
               if(invoice_aging_bucket=='Not Due' ):
                   
                 wherestr+=" where tf.Invoice_Aging_Bucket = 'Not Due' "
                
               elif(invoice_aging_bucket=='above 180 days'):
                   wherestr+=" where tf.Invoice_Aging_Bucket ='above 180 days' "
               else:
                   
                 wherestr+=" where  tf.Invoice_Aging_Bucket = '{}-{}' ".format(bucket_from,bucket_to)
               flag=1
           if(flag==1):
               if(invoice_aging_bucket=='Not Due' ):
                     print("**********************hello")
                     wherestr+=" and tf.Invoice_Aging_Bucket = 'Not Due' "
                    
               elif(invoice_aging_bucket=='above 180 days'):
                   wherestr+=" and tf.Invoice_Aging_Bucket ='above 180 days' "
               else:
                   print("**********************hi")
                   wherestr+=" and  tf.Invoice_Aging_Bucket = '{}-{}' ".format(bucket_from,bucket_to)
 
    
    
    query='''select * from (select  Customer_Number, Customer_Name,Sales_Order_Number, Invoice_Number,Invoice_Posting_date,Item_Number,Amount,Invoice_Aging, CASE WHEN  split_part(invoice_aging_bucket,'-',1)::int >90 and split_part(invoice_aging_bucket,'-',2)::int <=180 then '91-180'  WHEN split_part(invoice_aging_bucket,'-',1)::int >180  then 'above 180 days' 
			   WHEN  split_part(invoice_aging_bucket,'-',1)::int =0 and split_part(invoice_aging_bucket,'-',2)::int =0 then 'Not Due' 
			   ELSE invoice_aging_bucket   END  as Invoice_Aging_Bucket from 

( SELECT b.KUNNR Customer_Number,n.NAME1 Customer_Name,b.BELNR Sales_Order_Number, b.VBEL2 Invoice_Number,
b.BUDAT Invoice_Posting_date,b.BUZEI Item_Number,b.WRBTR Amount,extract(day from CURRENT_DATE-(ZFBDT::date+((concat(ZBD1T::varchar,' day'))::varchar)::INTERVAL)) Invoice_Aging,
CONCAT((CASE WHEN floor(extract(day from CURRENT_DATE-(ZFBDT::date+((concat(ZBD1T::varchar,' day'))::varchar)::INTERVAL))/30) <= 0 THEN 0 ELSE (floor(extract(day from CURRENT_DATE-(ZFBDT::date+((concat(ZBD1T::varchar,' day'))::varchar)::INTERVAL))/30)*30)+1 END)::VARCHAR,
'-',(CASE WHEN ceil(extract(day from CURRENT_DATE-(ZFBDT::date+((concat(ZBD1T::varchar,' day'))::varchar)::INTERVAL))/30) <= 0 THEN 0 ELSE (ceil(extract(day from CURRENT_DATE-(ZFBDT::date+((concat(ZBD1T::varchar,' day'))::varchar)::INTERVAL))/30)*30) END)::VARCHAR) Invoice_Aging_Bucket



FROM invoice.BSID b INNER JOIN invoice.KNA1 n ON n.KUNNR=b.KUNNR )  as  tbl )as tf  {}'''.format(wherestr)
    
    print(query)
    
    try:
        db.insert('rollback')
        df = pd.read_sql(query, con=con)
       
        df['invoice_aging']=df['invoice_aging'].astype(int)
       
        customer_name=list(set(df.customer_name))
        customer_name.append('All')
      
        invoice_aging_bucket_data=['Not Due','0-30 days','31-60 days','61-90 days','91-180 days','above 180 days','All']
        
        df['invoice_posting_date']=df['invoice_posting_date'].astype(str)
        df['invoice_posting_date'] = df['invoice_posting_date'].str.split(' ').str[0]  
        df['invoice_posting_date']=pd.to_datetime(df['invoice_posting_date'], format='%Y/%m/%d')
        df['invoice_posting_date']=df['invoice_posting_date'].astype(str)
        
        
        
        invoice_aging=list(set(df.invoice_aging))
        
        invoice_aging_from=invoice_aging
        invoice_aging_to=invoice_aging
        
        
        
        invoice_aging.sort()
        invoice_aging.append('All')
        
        if(search_string!="All" and search_string!='all' and search_string!=None):
                              df=df[df.eq(search_string).any(1)]
                             
        try:                        
          df=df.loc[lowerLimit:upperLimit]
        except:
            pass
        
        data=json.loads(df.to_json(orient='records'))
      
    
        return jsonify({"data":data,"customer_name":customer_name,"invoice_aging":invoice_aging,"invoice_aging_bucket_data":invoice_aging_bucket_data,"invoice_aging_from":invoice_aging_from,"invoice_aging_to":invoice_aging_to})
    except:
        return {"stataus":"sucess"},200

      
@taskbar_invoice_app.route('/order_status_delay',methods=['POST','GET'])

def order_status_delay():
    search_string=request.args.get("search_string")
    
    order_status=request.args.get("Order_Status")
    sold_to=request.args.get("Sold_To")
    ship_to=request.args.get("Ship_To")
    sales_doc_item_number=request.args.get("Sales_Doc_Number",type=int)
    
    
    
    wherestr=''
    flag=0
    
    
    if(order_status!='all' and order_status!=None):
       
        if(flag==0):
            wherestr+= ''' where order_status = '{}' '''.format(order_status)
        else:
            wherestr+=''' and  order_status ='{}' '''.format(order_status)
        flag=1
    if(sold_to!='all' and sold_to!=None ):
       
        if(flag==0):
            wherestr+=''' where C.KUNNR = '{}' '''.format(sold_to)
        else:
            wherestr+=''' and  C.KUNNR ='{}' '''.format(sold_to)
        flag=1
        
    if(ship_to!='all' and ship_to!=None):
        
        if(flag==0):
            wherestr+=''' where C.KUNNR = '{}' ''' .format(ship_to)
        else:
            wherestr+=''' and  C.KUNNR = '{}' '''.format(ship_to)
        flag=1
    
    if(sales_doc_item_number!='all' and sales_doc_item_number!=None ):
        
        if(flag==0):
            wherestr+=''' where A.POSNR = '{}' '''.format(sales_doc_item_number)
        else:
            wherestr+=''' and A.POSNR = '{}' '''.format(sales_doc_item_number)
        flag=1

    
    
    
    query='''select A.VBELN as sales_doc_number,
A.POSNR as sales_doc_item_number ,
null as order_status,
A.ZZLIEFDAT_01 as confirmed_delivery_date,  
A.WERKS as delivering_plant,
C.KUNNR as sold_to,
C.KUNNR as ship_to,
A.KWMENG as quantity,
B.BSTNK as customer_reference
from invoice.vbap A
inner join invoice.vbak B on (B.VBELN = A.VBELN)
inner join  invoice.ocp_vbpa C on (C.VBELN = A.VBELN) and  (C.PARVW = 'AG')
      or (C.VBELN = A.VBELN) and (C.PARVW = 'AG')
 or (C.VBELN = A.VBELN) and (C.PARVW = 'AG') or (C.VBELN = A.VBELN) and (C.PARVW = 'WE')
      or (C.VBELN = A.VBELN) and (C.PARVW = 'WE')
 or (C.VBELN = A.VBELN) and (C.PARVW = 'WE') {} '''.format(wherestr)
    print(query)
    try:
            df=pd.read_sql(query,con=con)
            
            if(search_string!="All" and search_string!='all' and search_string!='ALL' and search_string!= None):
                                      df=df[df.eq(search_string).any(1)]
                                      
            df_json=json.loads(df.to_json(orient='records'))
            
            
            order_status=list(set(df.order_status))
            sold_to=list(set(df.sold_to))
            ship_to=list(set(df.ship_to))
            
            return {"data":df_json,"oder_status":order_status,"sold_to":sold_to,"ship_to":ship_to },200
    
    except:
            return {"status":"failure"},500


 