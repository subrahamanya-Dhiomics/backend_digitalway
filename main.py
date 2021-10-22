# -*- coding: utf-8 -*-
"""
Created on Wed Oct 20 09:56:42 2021

@author: Administrator
"""


from flask import Flask
from flask_cors import CORS
from Alloy_scrap import scrap_app
from smb_phase1 import smb_app1
from smb_phase2 import smb_app2

app = Flask(__name__)
CORS(app)




app.register_blueprint(scrap_app)



app.register_blueprint(smb_app1)

app.register_blueprint(smb_app2)




if __name__ == '__main__':
    app.run()