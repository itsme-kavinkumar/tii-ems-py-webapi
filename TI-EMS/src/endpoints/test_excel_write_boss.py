import sys
import os
import datetime
from  datetime import date,timedelta
import time
import pymysql
import pytz
import json
import psutil
import smtplib
from os.path import basename
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email import encoders
import requests
from openpyxl import load_workbook

db = pymysql.connect(host="localhost", user="root",passwd="", db="ems_v1" , port= 3306)
cursor = db.cursor(pymysql.cursors.DictCursor)


def YtdReportMail():
    try: 
        current_date = datetime.datetime.now()
        current_month = current_date.month
        dir = r'C:\Users\kavin\Ausweg\documnets\main\Auweg_project\Projects\TI-EMS-Git\AIC_IIOT_EMS_PY_TI\TI-EMS\src\endpoints\attachments'
        attachment_type =[ 'provision', 'both']
        attachments = []
        from_mail = 'iiot.support@ausweginfocontrols.com'
        password = 'AiC@I!oT$2024'
        to_mail = 'kavinkumar8696@gmail.com'
        cc_mail = 'kavin@ausweginfocontrols.com,mohammedraquib721@gmail.com,jayapratha111998@gmail.com,vinothausweg@gmail.com'
        subject = 'Avadi Campus YTD Report.'
        email_content = 'Dear Sir,<br><br>Good Morning!!! <br><br> ' + '<br><br>' + 'I have attached the Avadi Campus YTD report<br><br><br>' + '<br><br>' + 'Thank You, <br><br><b>With Best Regards<b>,<br><br><b>EMS Team.<b>,'
        for data in attachment_type:

            params = {
                'campus_id':1,
                'month': (current_date.replace(day=1, month= current_month - 1 if data == 'provision' else current_month - 2 )).strftime("%d-%m-%Y"),
                'report_method':data,
                'user_login_id':1
            }
            print(params)
       
            response = requests.post(f'http://localhost:5002/campus_wise_power_consumption_units', data=params )
            print(response.json())
            print(response.text)

            workbook = load_workbook(os.path.join(dir, 'CampusWisePowerConsumption.xlsx'))
            attachments_path = os.path.join(dir, f'CampusWisePowerConsumption({data}).xlsx')
            workbook.save(attachments_path)
            workbook.close()
            attachments.append(attachments_path)

        print(f"To: {to_mail}")
        print(f"Cc: {cc_mail}")

        result =  SendMail(from_mail, password, to_mail, cc_mail,subject, email_content, attachments)
    except Exception as e:
        print('error-- ',str(e))



def SendMail(from_mail, password, to_mail, cc_mail, subject, body, attachments):

    smtp_server = 'smtp-mail.outlook.com'
    smtp_port = 587

    # Create a MIME message
    msg = MIMEMultipart()
    msg['From'] = from_mail
    msg['To'] = to_mail
    msg['Cc'] = cc_mail
    msg['Subject'] = subject

    # Attach the body of the email to the MIME message
    msg.attach(MIMEText(body, 'html'))

    # Attach files to the email
    for file_path in attachments:
        part = MIMEBase('application', 'octet-stream')
        try:
            with open(file_path, 'rb') as attachment:
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(file_path)}')
            msg.attach(part)
        except Exception as e:
            print(f'Failed to attach {file_path}. Error: {e}')

    # Combine the main recipient and the CC recipients
    recipients = to_mail.split(',') + cc_mail.split(',')

    # Connect to the SMTP server and send the email
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as smtp:
            smtp.starttls()  # Secure the connection
            smtp.login(from_mail, password)
            smtp.send_message(msg, from_mail, recipients)
            print("Mail sent successfully!")
    except Exception as e:
        print(f'Failed to send email to {to_mail}. Error: {e}')




