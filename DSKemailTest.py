# -*- coding: utf-8 -*-
"""
Created on Thu Jun  3 17:45:05 2021

@author: Hp
"""

# python_send_email.py

import os
import smtplib
from email.message import EmailMessage #new

message = EmailMessage()
message['Subject'] = 'Python Send Email'
message['From'] = 'intigralmailfetcher@gmail.com'
message['To'] = 'sureshkumar.durairaj@intigral.net'
message.set_content('This email is sent using python.')
message.add_alternative("""\
<!DOCTYPE html>
<html>
    <body>
        <h1>This is an HTML Email!</h1>
    </body>
</html>
""", subtype = 'html')
with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
    smtp.login('intigralmailfetcher@gmail.com', 'intigralmailfetcher_1')
    smtp.send_message(message)