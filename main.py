# -*- coding: utf-8 -*-
"""
Created on Wed Oct 20 09:56:42 2021

@author: subbu
"""


from flask import Flask
from flask_cors import CORS
from Alloy_scrap import scrap_app
from smb_phase1 import smb_app1
from smb_phase2 import smb_app2
from smb_phase3 import smb_app3
from smb_history import smb_history
from taskbar1 import taskbar1


app = Flask(__name__)
CORS(app)


app.register_blueprint(scrap_app)



app.register_blueprint(smb_app1)

app.register_blueprint(smb_app2)
app.register_blueprint(smb_app3)
app.register_blueprint(taskbar1)


app.register_blueprint(smb_history)

if __name__ == '__main__':
    # app.run(host='172.16.4.75', port=5000)
    app.run()