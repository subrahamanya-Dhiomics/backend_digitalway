# -*- coding: utf-8 -*-
"""
Created on Tue Nov  9 12:39:30 2021

@author: Administrator
"""
from flask import Flask, jsonify, request

from flask_cors import CORS
import json

app = Flask(__name__)
CORS(app)


@app.route('/smb_request', methods=['POST'])
def add_income():
    
    data=json.loads(request.data)
    print(data)
    
    
    return "success"
    
    
  
 




if __name__ == '__main__':
    app.run()