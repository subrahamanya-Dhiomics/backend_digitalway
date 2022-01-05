import os
import smtplib


email=os.environ.get("ymsyathish@gmail.com")
pwd=os.environ.get("offertool2w2016.")


smtp = smtplib.SMTP('smtp.gmail.com', 587)
smtp.ehlo()
smtp.starttls()
smtp.ehlo()
smtp.login(email,pwd)
subject='hi yahthish'
msg=f'Subject:{subject}'

smtp.sendmail(email,'yathish.susandhi@gmail.com',msg)
