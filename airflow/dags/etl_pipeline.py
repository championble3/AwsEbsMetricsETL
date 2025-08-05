import boto3
import pandas as pd
from dotenv import load_dotenv
import os 
from datetime import datetime, timedelta
import snowflake.connector
from airflow.decorators import task, dag

load_dotenv()
AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION=os.getenv("AWS_REGION")

SNOWFLAKE_USER=os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD=os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT=os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE=os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE=os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA=os.getenv("SNOWFLAKE_SCHEMA")

session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        region_name=AWS_REGION
                        )

#cloudwatch = session.client('cloudwatch')

def response_cloudwatch(volume_type,cloudwatch):
    response = cloudwatch.get_metric_statistics(
        Namespace='AWS/EBS',
        MetricName=volume_type,
        Dimensions=[
            {'Name': 'VolumeId', 'Value': 'vol-09af009ff9244a632'}  
        ],
        StartTime = datetime.utcnow() - timedelta(hours=24),
        EndTime = datetime.utcnow(),
        Period = 360,
        Statistics=['Sum']
    )
    return response

def volume_values():
    volume_list = ['VolumeWriteBytes','VolumeWriteOps','VolumeIdleTime','VolumeQueueLength','Average write latency']
    data_dict = {
        "volume": [],
        "time": [],
        "value": []
    }

    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    cloudwatch = session.client('cloudwatch', region_name=AWS_REGION)
    
    for volume in volume_list:
        response = response_cloudwatch(volume, cloudwatch)
        datapoints = response['Datapoints']
        for point in datapoints:
            data_dict["volume"].append(volume)
            data_dict["time"].append(point['Timestamp'])
            data_dict["value"].append(point['Sum'])
    
    return data_dict

### Upload data to Snowflake
def snowflake_connection(SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    return conn

def snowflake_staging():
    conn = snowflake_connection(SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA)
    cursor = conn.cursor()

    data_dict = volume_values()

    does_exist = """
    SELECT COUNT(*) 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME = 'STG_VOLUMES'
    """
    cursor.execute(does_exist)
    result = cursor.fetchone()
    if result[0] == 0:
        cursor.execute('''
                CREATE TABLE stg_volumes(
                    ID INT IDENTITY(1,1),
                    volume STRING,
                    time TIMESTAMP,
                    value FLOAT 
                    )
        ''')
    cursor.execute('''DELETE FROM stg_volumes''')
    print('Previous data deleted')
    for i in range(len(data_dict['volume'])):
        try:
            volume = data_dict['volume'][i]
            timestamp = data_dict['time'][i]
            value = data_dict['value'][i]
            cursor.execute(
                        "INSERT INTO stg_volumes (volume, time, value) VALUES (%s, %s, %s)",
                        (volume, timestamp, value)
                    )
            print('Data inserted')
        except Exception as e:
            print(f'Error {e}')

    conn.commit()
    cursor.close()
    conn.close()

def snowflake_dwh():
    conn = snowflake_connection(SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA)
    cursor = conn.cursor()

    does_exist = """
    SELECT COUNT(*) 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME = 'DWH_VOLUMES'
    """
    cursor.execute(does_exist)
    result = cursor.fetchone()
    if result[0] == 0:
        cursor.execute('''
                CREATE TABLE dwh_volumes(
                    ID INT,
                    Volume STRING,
                    Time TIMESTAMP,
                    Day INT,
                    Weekday INT,
                    Month INT,
                    Year INT,
                    VolumeValue FLOAT 
                    )
        ''')
    try:
        cursor.execute(
                    '''MERGE INTO dwh_volumes AS target
                            USING (
                            SELECT 
                                ID, 
                                volume, 
                                time, 
                                value,
                                DAY(time) AS day,
                                DAYOFWEEK(time) AS weekday,
                                MONTH(time) AS month,
                                YEAR(time) AS year
                            FROM stg_volumes
                            ) AS source
                            ON target.ID = source.ID
                            WHEN MATCHED THEN
                            UPDATE SET VolumeValue = source.value
                            WHEN NOT MATCHED THEN
                            INSERT (ID, Volume, Time, Day, Weekday, Month, Year, VolumeValue)
                            VALUES (
                                source.ID, 
                                source.volume, 
                                source.time, 
                                source.day, 
                                source.weekday, 
                                source.month, 
                                source.year, 
                                source.value
                            );
                    ''')
        print('Data inserted into dwh')
    except Exception as e:
        print(f'Error {e}')

    conn.commit()
    cursor.close()
    conn.close()

@task()
def snowflake_staging_task():
    snowflake_staging()

@task()
def snowflake_dwh_task():
    snowflake_dwh()



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025,7,28),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'depends_on_past': False
}

@dag(
    dag_id='etl_aws_ebs',
    default_args=default_args,
    description='ETL Pipeline for AWS EBS data',
    schedule_interval=None,
    catchup=False,
    tags=['etl','aws','snowflake'],
)
def aws_etl_dag():
    staging = snowflake_staging_task()
    dwh = snowflake_dwh_task()
    dwh

dag = aws_etl_dag()
