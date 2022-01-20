
from flask import Flask
from flask_cors import CORS
from Alloy_scrap import scrap_app
from smb_phase1 import smb_app1
from smb_phase2 import smb_app2
from smb_phase3 import smb_app3
from reset_password import reset_password
from smb_history import smb_history


app = Flask(__name__)
CORS(app)

app.config["environment_url"] = "https://smbprice.dcc-am.com/auth/reset-password?user_id"
app.register_blueprint(scrap_app)  #https://smbprice.dcc-am.com/auth/reset-password?user_id

app.config["LOGIN_SECRETKEY"] = "login_secret_key"

app.register_blueprint(smb_app1)
app.register_blueprint(reset_password)

app.register_blueprint(smb_app2)
app.register_blueprint(smb_app3)


app.register_blueprint(smb_history)



if __name__ == '__main__':
    # app.run(host='172.16.4.75', port=5000)
    app.run()