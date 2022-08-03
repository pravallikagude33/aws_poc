#Importing required packages
from datetime import timedelta
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
import boto3
import json
import requests
import logging
import time,decimal
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrModifyClusterOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor
from airflow.models.xcom import XCom
from pyarrow.parquet import ParquetDataset
import pandas as pd
region_name='us-west-1'
#Reading app configuration file
def reading_config_file(**kwargs):
  # Retrieving value from dag_run.conf
    dag_run = kwargs.get('dag_run')
    app_config_path = dag_run.conf['app_config']    
    path_list = app_config_path.replace(":","").split("/")
    s3 = boto3.resource(path_list[0])
    obj = s3.Object(path_list[2],"/".join(path_list[3:]))
  # Reading data from app-config file
    data = obj.get()['Body'].read().decode('utf-8')
    jsondata = json.loads(data)
    return jsondata
#copying data from landing zone to raw zone
def copy_data_landing_to_raw(**kwargs):
    # fetching values from reading_config_file function 
    ti = kwargs['ti']
    jsonData = ti.xcom_pull(task_ids='read_config_task')
    datasets = jsonData['datasets']
    print(datasets)
    s3_client = boto3.client('s3')
    # Bucket and Key Names
    actives_source = jsonData['ingest-Actives']['source']['data-location'].replace(":","").split("/")
    actives_destination = jsonData['ingest-Actives']['destination']['data-location'].replace(":","").split("/")
    viewership_source = jsonData['ingest-Viewership']['source']['data-location'].replace(":","").split("/")
    viewership_destination = jsonData['ingest-Viewership']['destination']['data-location'].replace(":","").split("/")
    
    for dataset in datasets:
        #copying actives dataset
        if dataset == "Actives":
           s3_client.copy_object(
                                CopySource = {'Bucket': actives_source[2], 'Key':"/".join(actives_source[3:])},
                                Bucket = actives_destination[2],
                                Key = "/".join(actives_destination[3:])
                                )
        elif dataset == 'Viewership':
            s3_client.copy_object(
                                CopySource = {'Bucket': viewership_source[2], 'Key':  "/".join(viewership_source[3:])},
                                Bucket = viewership_destination[2],
                                Key = "/".join(viewership_destination[3:])
                                )

        else:
            pass
            print("data not copied")

def pre_validation(**kwargs):
    ti=kwargs['ti']
    
    #boto3 s3 client object
    s3_client = boto3.client('s3')
    dag_run = kwargs.get('dag_run')
    dataset_path = dag_run.conf['dataset_path']
    jData = ti.xcom_pull(task_ids="read_config_task")
    actives_path=jData['masked-Actives']['source']['data-location'].replace(":","").split("/")
    print(actives_path)
    viewership_path=jData['ingest-Viewership']['destination']['data-location'].replace(":","").split("/")
    bucket = actives_path[2]
    print(bucket)
    actives_destination_key = '/'.join(actives_path[3:])
    print(actives_destination_key )
    viewership_destination_key = '/'.join(viewership_path[3:])
    result = s3_client.list_objects_v2(Bucket=bucket, Prefix=actives_destination_key)
    pre_validation_data={}
    if 'Contents' in result:
        print("Key exists in the bucket.")
    else:
        print("Key doesn't exist in the bucket.")		
    dataset = ParquetDataset(dataset_path,use_legacy_dataset=False).read_pandas().to_pandas()
    dataset.info()
    nrows =int(dataset.count()[0])
    print(nrows)
    pre_validation_data['rawzone_data_count'] = nrows
    return pre_validation_data
    
def post_validation(**kwargs):
    ti=kwargs['ti']
    jData = ti.xcom_pull(task_ids="read_config_task")
    raw_count= ti.xcom_pull(task_ids="pre_validation")
    print(raw_count)
    s3_client = boto3.client('s3')
    actives_path=jData['masked-Actives']['destination']['data-location'].replace(":","").split("/")
    dataset_path=jData['masked-Actives']['destination']['data-location']
    
    bucket = actives_path[2]
    actives_destination_key = '/'.join(actives_path[3:])
    result = s3_client.list_objects_v2(Bucket=bucket, Prefix=actives_destination_key)
    if 'Contents' in result:
        print("Key exists in the bucket.")
    else:
        print("Key doesn't exist in the bucket.")	
    dataset = ParquetDataset(dataset_path,use_legacy_dataset=False).read_pandas().to_pandas()
    mrows =int(dataset.count()[0])
    print(mrows)
    if raw_count['rawzone_data_count']==mrows:
        print("yes")
    else:
        print("no")
    print(dataset.info())
    transformation_cols = jData['masked-Actives']['transformation-cols']
    for col, val in transformation_cols.items():
        #     print(col)
        if 'DecimalType' in val:
            if isinstance(dataset[col][0], decimal.Decimal):
                print("same")
                continue
            else:
                print("Data mismatch for {}".format(col))
        if 'StringType' in val:
            if pd.api.types.is_string_dtype(dataset[col]):
                print("same")
                continue
            else:
                print("Data mismatch for {}".format(col))
        else:
            print("No data in the column")
    
with DAG(
    dag_id='dag_4_test1',
    start_date= datetime(2022,7,1),
    schedule_interval='@once',
    catchup = False
) as dag:


    fetch_config_task = PythonOperator(task_id = 'read_config_task',python_callable = reading_config_file)
    copy_2_raw_task= PythonOperator(task_id = 'copy_2_raw_task' ,python_callable = copy_data_landing_to_raw)
    pre_validation = PythonOperator(
    task_id='pre_validation',
    python_callable=pre_validation)
    post_validation=PythonOperator(task_id = 'post_validation' ,python_callable = post_validation)
     
    fetch_config_task>>copy_2_raw_task>>pre_validation>>post_validation