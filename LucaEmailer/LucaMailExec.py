#!/usr/bin/python

import smtplib

sender = 'do-not-reply@cnrl.com'
receivers = ['seenuvasan.rajan@cnrl.com']
smtp_server = 'smtp.albian.ca'
smtp_port = 25

message = """From: From Person <do-not-reply@cnrl.com>
To: To Person <seenuvasan.rajan@cnrl.com>
Subject: SMTP e-mail test

This is a test e-mail message.
"""

try:
    smtpObj = smtplib.SMTP(host=smtp_server, port=smtp_port)
    smtpObj.sendmail(sender, receivers, message)
    print("Successfully sent email")
except Exception as err:
    print("Error: unable to send email")
    print(err)
