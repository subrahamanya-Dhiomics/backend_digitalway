# -*- coding: utf-8 -*-
"""
Created on Mon Dec 20 09:30:46 2021

@author: Administrator
"""

from flask import Flask
from flask_cors import CORS
from Alloy_scrap import scrap_app
from smb_phase1 import smb_app1
from smb_phase2 import smb_app2
from smb_phase3 import smb_app3
from smb_history import smb_history
from taskbar1 import taskbar1
from web_api_response import web_api_response

# Changes
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash



app = Flask(__name__)
auth = HTTPBasicAuth()
CORS(app)



users = {
    "john": generate_password_hash("hello"),
    "susan": generate_password_hash("bye"),

    
}

@auth.verify_password
def verify_password(username, password):
    if username in users and \
            check_password_hash(users.get(username), password):
        return username


@app.route('/')
@auth.login_required
def index():
   app.register_blueprint(scrap_app)
   app.register_blueprint(smb_app1)
   app.register_blueprint(smb_app2)
   app.register_blueprint(smb_app3)
   app.register_blueprint(taskbar1)
   app.register_blueprint(web_api_response)
   app.register_blueprint(smb_history)
   return f"Hello, {auth.current_user()}!"

if __name__ == '__main__':
   app.run()
   