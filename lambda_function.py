import json
import pandas as pd
import requests
import boto3
import constant as ct
import datetime
import time
from io import StringIO

sns = boto3.client("sns", region_name="us-west-2")
ssm = boto3.client("ssm", region_name="us-west-2")
s3_resource = boto3.resource('s3')
bucket = 'vno-packages'
a = ssm.get_parameter(Name=ct.urlapi, WithDecryption=True)["Parameter"]["Value"]
# a = "https://results.us.securityeducation.com/api/reporting/v0.1.0/phishing"
b = {'x-apikey-token': 'YV5iYWWji13z25LX/ZAQOwmHKmHqdpQk8pQzIcSQ5hGl3cpog'}
s3_prefix = "result/csvfiles"
def get_datetime():
    dt = datetime.datetime.now()
    return dt.strftime("%Y%m%d"), dt.strftime("%H:%M:%S")
datestr, timestr = get_datetime()
fname = f"data_api_tripler_{datestr}_{timestr}.csv"
file_prefix = "/".join([s3_prefix, fname])

def send_sns_success():
    success_sns_arn = ssm.get_parameter(Name=ct.SUCCESSNOTIFICATIONARN, WithDecryption=True)["Parameter"]["Value"]
    component_name = ct.COMPONENT_NAME
    env = ssm.get_parameter(Name=ct.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
    success_msg = ct.SUCCESS_MSG
    sns_message = (f"{component_name} :  {success_msg}")
    print(sns_message, 'text')
    succ_response = sns.publish(TargetArn=success_sns_arn,Message=json.dumps({'default': json.dumps(sns_message)}),
        Subject= env + " : " + component_name,MessageStructure="json")
    return succ_response
def send_error_sns():
   
    error_sns_arn = ssm.get_parameter(Name=ct.ERRORNOTIFICATIONARN)["Parameter"]["Value"]
    env = ssm.get_parameter(Name=ct.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
    error_message=ct.ERROR_MSG
    component_name = ct.COMPONENT_NAME
    sns_message = (f"{component_name} : {error_message}")
    err_response = sns.publish(TargetArn=error_sns_arn,Message=json.dumps({'default': json.dumps(sns_message)}),    Subject=env + " : " + component_name,
        MessageStructure="json")
    return err_response

r = requests.get(a,headers=b)
while r.status_code == 403:
    print('URL is not getting hit')
    time.sleep(2)
    send_error_sns()
if r.status_code != 403:
    print('URL is hit')
    c = r.json()
    df = pd.DataFrame(c['data'])
    print(df)
    df1 = df[df.columns.drop(['attributes'])]
    print(df1)
    a = [d.values() for d in df.attributes]
    b = [d.keys() for d in df.attributes]
    print(a)
    print(b)
    # pd.reset_option('max_columns')
    df2 = pd.DataFrame(a,columns=['First_name', 'Last_name', 'Email_address', 'useractiveflag', 'userdeleteddate', 'senttimestamp', 'Time_stamp', 'eventtype', 'campaignname', 'autoenrollment', 'campaignstartdate', 'campaignenddate', 'campaigntype', 'campaignstatus', 'templatename', 'templatesubject', 'assessmentisarchived', 'usertags', 'sso_id'])
    print(df2)
    result = pd.concat([df1, df2], axis=1, join="outer")
    # result1 = result[['First_name'],['Last_name']]
    result1 = result.loc[:,'First_name': 'Time_stamp'] # : (isto) method used for taking the columns
    print(result1)
    print(result)
    # send_sns_success()
    # print('Email Delivered')
def lambda_handler(event, context):
    csv_buffer=StringIO()
    result.to_csv(csv_buffer)
    s3_resource.Object(bucket, file_prefix).put(Body=csv_buffer.getvalue())
    print('CSV files written')
    ####SUCCESS-SNS-NOTIFICATION ACTIVATED#####    
    send_sns_success()
    print('file has been written successfully')
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }