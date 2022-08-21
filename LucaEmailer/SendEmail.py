import smtplib
from email.message import EmailMessage
import ssl
import LucaEmailer.PrepEmail as access_mail

conn_obj = access_mail.CrtConnObject()
mail, msg = access_mail.Data_Retrival(conn_obj)

for i,j in zip(mail,msg):
    sender = "robypratheep@gmail.com"   #  for outlook ---> sender@outlook.com
    reciever = i   # it may want to be gmail ---->  destination@gmail.com
    password = 'vino4682'
    msg_b = j           # body
    msg_body = str(msg_b)

    text = 'this is  sample message'    # subject
    msg = EmailMessage()
    msg['subject'] = text
    msg['from'] = sender
    msg['to'] = reciever
    msg.set_content(msg_body)     #  emphasises all the mailing information


    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:  #  if it is outlook service ---> with smtplib.SMTP_SSL('smtp-mail.outlook.com',465) as smtp
        smtp.login(sender, password)
        smtp.send_message(msg)

#!/usr/bin/python
import smtplib


def send_email(msg_receivers, msg_subject, msg_alert_details_for_email, msg_alert_recommended_steps):
    sender = 'alert_manager@cnrl.com'
    receivers = msg_receivers  # ['seenuvasan.rajan@cnrl.com']
    smtp_server = 'smtp.albian.ca'
    smtp_port = 25

    email_from = f'From: From Person <alert_manager@cnrl.com> \n'
    email_receivers = f'To: To Person <{receivers}> \n'
    email_subject = f'Subject: {msg_subject} \n'
    email_body = f'{msg_subject}\n Alert Details :\n{msg_alert_details_for_email}\n Recommendations :\n{msg_alert_recommended_steps}  '

    msg = email_from + email_receivers + email_subject + email_body
    print(msg)

    message = """From: From Person <do-not-reply@cnrl.com>
    To: To Person <seenuvasan.rajan@cnrl.com>
    Subject: SMTP e-mail test
    
    This is a test e-mail message.
    """

    try:
        smtpObj = smtplib.SMTP(host=smtp_server, port=smtp_port)
        smtpObj.sendmail(sender, receivers, msg)
        print("Successfully sent email")
    except Exception as err:
        print("Error: unable to send email")
        print(err)
