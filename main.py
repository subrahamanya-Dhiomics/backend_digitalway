
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
# from taskbar1 import taskbar1
import pandas as pd
from user_management import user_management_app
# from taskbar_invoice import taskbar_invoice_app

from SMB_Generic import generic

from reset_password import reset_password 

app = Flask(__name__)
CORS(app)
app.register_blueprint(reset_password)

app.register_blueprint(scrap_app)
app.register_blueprint(smb_app1)
app.register_blueprint(smb_app2)
app.register_blueprint(smb_app3)
# app.register_blueprint(taskbar1)
app.register_blueprint(user_management_app)
# app.register_blueprint(taskbar_invoice_app)
app.register_blueprint(smb_history)
app.register_blueprint(generic)


# con1=psycopg2.connect(dbname='offertool',user='pgadmin',password='Sahara_17',host='offertool2-qa.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')
# con = psycopg2.connect(dbname='offertool',user='postgres',password='ocpphase01',host='ocpphase1.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')

app.config["environment_url"] = "https://smbprice.dcc-am.com/auth/reset-password?user_id"
app.config['SECRET_KEY'] = 'YOU_SECRET_KEY'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1000 * 1000
app.config['CON']=''
app.config['mypassword']='digitalway'


db=Database()

# db connection opening and closing before and after the every api calls 

@app.before_request
def before_request():
   
    current_app.config['CON']  = psycopg2.connect(dbname='offertool', user='pgadmin', password='Sahara_17',
                       host='offertool2-qa.cjmfkeqxhmga.eu-central-1.rds.amazonaws.com')
    

@app.after_request
def after_request(response):
     current_app.config['CON'].close()
     return response
 



@app.route('/login', methods=['POST','GET'])
def login():
    username=request.args.get('username')
    password=request.args.get('password')
    
    
    
    try:
        
     query="select distinct(1) from  user_management_ocp.user_details  where user_name='{}' and password='{}'".format(username,password)
     status=db.query(query)[0][0]
     
    except:
        status=0
        
    
    if status==1:
        
        menu_query=''' select menu from user_management_ocp.group_access a inner join user_management_ocp.user_details u on a.group_code=u.group_id where user_name='{}' '''.format(username)
        print(menu_query)
        menu_items=db.query(menu_query)[0][0]
        menu_items.append('test')
        
       
         
        data=db.query(" select data from user_management_ocp.menudata where menu in  {}".format(tuple(menu_items)))
        
        menu_list=[]
        for m in data:
            item=json.loads(m[0])
            menu_list.append(item)
        
        df=pd.read_sql("select * from  user_management_ocp.user_details  where user_name='{}'".format(username),con=current_app.config['CON'])
        user=json.loads(df.to_json(orient='records'))[0]
        
       
        token = jwt.encode({
            'user':username
        },
            app.config['SECRET_KEY'])
        
        return jsonify({'token': token,'user':user,'sidebar_json':menu_list}),200
    else:
        return make_response('Unable to verify',  {'WWW-Authenticate': 'Basic realm: "Authentication Failed "'}),500


def range_logic(data,typ):
    if(typ=='len'):
        
        length_string='''  ("Length"='*' or "Length"='{}') and CASE
    		WHEN "Length To"~E'^\\\d+$' THEN
    			CAST ("Length To" AS INTEGER) >={}
    			else "Length To"='*'
    			
    		end
    			and
    case	WHEN "Length From"~E'^\\\d+$' THEN
    			CAST ("Length From" AS INTEGER) <={}
    			
    			else "Length From"='*'
    			
    		end'''.format(data['Length'],data['Length'],data['Length'])
        return length_string
    else:
        tonnage_string='''CASE
    		WHEN "Tonnage To"~E'^\\\d+$' THEN
    			CAST ("Tonnage To" AS INTEGER) >={}
    			else "Tonnage To"='*'
    			
    		end
    			and
    case	WHEN "Tonnage From"~E'^\\\d+$' THEN
    			CAST ("Tonnage From" AS INTEGER) <={}
    			
    			else "Tonnage From"='*'
    			
    		end'''.format(data['Tonnage'],data['Tonnage'])
    			
        data.pop('Tonnage')
        return tonnage_string
    

@app.route("/SMBMinibarService",methods=['GET','POST'])
def web_api():
    
    data=str( request.data)
   
    # text_file = open("data.txt", "w")


    # text_file.write(data)
   
    # text_file.close()
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
    
    if('Length' in data):
        if(data['Length'] !='*'):
         wherestr=wherestr+range_logic(data,'len')
         data.pop('Length')
         k=1
         
    if('Tonnage' in data):
        if(data['Tonnage']!='*'):
            wherestr=wherestr+range_logic(data,'ton')
            k=1
        else:
            data.pop('Tonnage')
            
  
    for i in data:
        if(k==0):
            wherestr=wherestr+ "("+    '"' + i +'"'    +   " = "  +  "'" +str(data[i])+"'"   + " or " +   '"' + i +'"' + " = "+ "'"+ "*" +"'"+     ")"
            k=1
        elif(k==1):
            wherestr=wherestr + " and " +"("+    '"' + i +'"'    +   " = "  +  "'" +str(data[i])+"'"   + " or " +   '"' + i +'"' + " = "+ "'"+ "*" +"'"+     ")"
            
    query=query+wherestr

    query+=" and active=1 order by sequence_id asc limit 1"
    print(query)
    data=db.query(query)
       
    df=pd.read_sql(query,con=current_app.config['CON'])
    df_json=json.loads(df.to_json(orient='records'))
           
    return {"data":df_json,"status":"sucess"}
   
        
        
 
if __name__ == '__main__':
    # app.run(host='172.16.4.190')
    app.run()
   