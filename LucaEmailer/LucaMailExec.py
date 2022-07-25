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
