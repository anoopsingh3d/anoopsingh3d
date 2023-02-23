# This script will set report month to previous month in YYYYMM format
from datetime import date,timedelta,datetime
import os
from smb.SMBConnection import SMBConnection
import socket
import pandas as pd
report_month = (date.today().replace(day=1) - timedelta(days=1))
report_month = (report_month.strftime("%Y%m"))
print(report_month)

# COMMAND ----------

from pyspark.sql.functions import date_format,to_date
#Get data from apple summary table and create dataframe and then changing date format
apple_df = spark.sql("select * from accounting_dm.bia_apple_sales_revenue_monthly where booking_date between (Last_day(Add_months(current_date(), -2))+1) and Last_day(Add_months(current_date(), -1)) ") 
apple_df = apple_df.withColumn("Booking_date",date_format(to_date(apple_df.Booking_date,'yyyy-MM-dd'),'dd.MM.yyyy'))
apple_df = apple_df.fillna('')

# COMMAND ----------

#Get data from google summary table , create dataframe and then changing date format
google_df = spark.sql("select * from accounting_dm.bia_google_sales_revenue_monthly where transaction_date between (Last_day(Add_months(CURRENT_DATE(), -2))+1) and Last_day(Add_months(CURRENT_DATE(), -1))") 
google_df = google_df.withColumn("transaction_date",date_format(to_date(google_df.transaction_date,'yyyy-MM-dd'),'dd.MM.yyyy'))
google_df = google_df.fillna('')


# apple file write
os.chdir('/home')
l_dir = os.listdir()
for folder in l_dir:
    try:
        os.chdir('/home/'+ folder)
        if not os.path.exists('apple_sales_revenue'):
            os.mkdir('apple_sales_revenue')           
        break
    except:
        continue
        
apple_pd_df = apple_df.toPandas()        
apple_pd_df.to_csv('apple_sales_revenue/' + 'apple_sales_revenue_'+report_month+'_'+ datetime.now().strftime("%Y%m%d_%H%M%S") + '.csv',
   sep=",", encoding='utf-8',  header=True)


# COMMAND ----------

# google file write
os.chdir('/home')
l_dir = os.listdir()
for folder in l_dir:
    try:
        os.chdir('/home/'+ folder)
        if not os.path.exists('google_sales_revenue'):
            os.mkdir('google_sales_revenue')  
        break
    except:
        continue
        
google_pd_df = google_df.toPandas()        
google_pd_df.to_csv('google_sales_revenue/' + 'google_sales_revenue_'+report_month+'_'+ datetime.now().strftime("%Y%m%d_%H%M%S") + '.csv',
   sep=",", encoding='utf-8', header=True)

# COMMAND ----------

 # Coonect to sage machine and upload file
# client_machine_name can be an arbitary ASCII string
# server_name should match the remote machine name, or else the connection will be rejected

client_machine_name = socket.gethostname()

userID=smb_user_id
password=smb_password
server_name = smb_server_name
server_ip = smb_server_ip

conn = SMBConnection(userID, password, client_machine_name, server_name, is_direct_tcp=True, use_ntlm_v2 = True) 
assert conn.connect(server_ip, 445) #  try 445 or 139

# COMMAND ----------

#Write apple report to sage
os.chdir('/home')
# go to drivernode (worker nodes seem not to be accessible)
# I could not find a way of finding the drivernode, therefore I'm going though all folders
# and try to write. If successful, this folder is taken
l_dir = os.listdir()
for folder in l_dir:
    try:
        os.chdir('/home/'+ folder)
        if os.path.exists('apple_sales_revenue'):
            os.chdir('./apple_sales_revenue/')
            break
    except:
        continue


list_of_files = os.listdir()
print(os.getcwd())
print(list_of_files)   


# # #go through content of folder and write to smb:
for file in list_of_files:
    with open(file, 'rb') as f:
        print(file)
        test = conn.storeFile('DataBricks','APPLE_REPORT/'+ file, f )
        
    


# COMMAND ----------

# #check whether apple file arrived in the smb:      
smb_files = conn.listPath('DataBricks', 'APPLE_REPORT')
for smb_file in smb_files:
    print(smb_file.filename) 



#Write google report to sage
os.chdir('/home')

l_dir = os.listdir()
for folder in l_dir:
    try:
        os.chdir('/home/'+ folder)
        if os.path.exists('google_sales_revenue'):
            os.chdir('./google_sales_revenue/')
            break
    except:
        continue


list_of_files = os.listdir()
print(os.getcwd())
print(list_of_files)   


#go through content of folder and write to smb:
for file in list_of_files:
    with open(file, 'rb') as f:
        print(file)
        test = conn.storeFile('DataBricks','GOOGLE_REPORT/'+ file, f )
        

# COMMAND ----------

# #check whether google file arrived in the smb:      
smb_files = conn.listPath('DataBricks', 'GOOGLE_REPORT')
for smb_file in smb_files:
    print(smb_file.filename) 


