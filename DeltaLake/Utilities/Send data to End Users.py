# Databricks notebook source
pip install email

# COMMAND ----------

# MAGIC %pip install --upgrade pip

# COMMAND ----------

pip install smtplib

# COMMAND ----------

import email,smtplib,ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

# COMMAND ----------

# Databricks notebook source
import email,smtplib,ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import os

sender_email="raunaksinghmatharu@gmail.com"
password=dbutils.secrets.get(scope = "scoperaunak", key = "outlook-password")
receiver_email="matharu.r@northeastern.edu"
month=datetime.today().month
year=datetime.today().year
subject="Formula 1 result data {0} {1}".format(month,year)
body="Please find out formula 1 result data in CSV file attached in the email"
message=MIMEMultipart()
message["From"]=sender_email
message["To"]=receiver_email
message["Subject"]=subject
message["Bcc"]=receiver_email

message.attach(MIMEText(body,'plain'))
df=spark.sql("""select  b.full_name,total_points from gold.fact a inner join gold.dim_driver b on a.driverId=b.driverId where year(date)=2022 and month(date)=08 order by total_points desc limit 10""")

# Create the directory if it doesn't exist
#directory = '/Workspace/Users/neha3193m@gmail.com/DeltaLake/csv_files'
directory ='/Workspace/Repos/neha3193m@gmail.com/Databricks_f1_etl/DeltaLake/csv_files'
if not os.path.exists(directory):
    os.makedirs(directory)

df.toPandas().to_csv(directory + '/result.csv',index=False)
with open(directory + '/result.csv','rb') as attachment:
    part=MIMEBase("application","octet-stream")
    part.set_payload(attachment.read())
encoders.encode_base64(part)
file_name='rsult.csv'
part.add_header("Content-Disposition",f"attachment; filename={file_name}")
message.attach(part)
text=message.as_string()
context=ssl.create_default_context()
with smtplib.SMTP_SSL("smtp.gmail.com",465,context=context) as server:
    server.login(sender_email,password)
    server.sendmail(sender_email,receiver_email,text)

# COMMAND ----------

# MAGIC %sql
# MAGIC select  b.full_name,total_points from gold.fact a inner join gold.dim_driver b on a.driverId=b.driverId  where year(date)=2022 and month(date)=08 order by total_points desc limit 10