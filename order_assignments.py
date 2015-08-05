from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from airflow.hooks import PostgresHook, S3Hook
from datetime import datetime, timedelta
import csv
import os

start_date = datetime.utcnow() - timedelta(days=10)
end_date = start_date + timedelta(days=20)
aws_key = os.environ.get('aws_key')
aws_pass = os.environ.get('aws_pass')

default_args = {
    'owner': 'naveen',
    'depends_on_past': True,
    'start_date': start_date,
    'end_date': end_date,
    'schedule_interval': timedelta(days=0,minutes=5)
    # 'email': ['naveen.lekkalapudi@getbellhops.com'],
    # 'email_on_failure': False,
    # 'retries': 1,
    # 'retry_interval': timedelta(minustes=5),
    # 'sla' : timedelta()   # Time by which job is expected to succeed.
}

dag = DAG('get_orders_from_postgres_single', default_args=default_args)

def postgres_call(sql):
    pghook = PostgresHook(postgres_conn_id="naveen_ngrok_postgres")
    return pghook.get_records(sql)

def redshift_call(sql, type_='run'):
    pghook = PostgresHook(postgres_conn_id="naveen_redshift")
    if type_ == 'run':
        return pghook.run(sql)
    else:
        return pghook.get_records(sql)

def s3_call(file_, bucket_name):
    s3hook = S3Hook(s3_conn_id="naveen_s3")
    s3hook.load_file(
        filename=file_,
        key='latest',
        bucket_name=bucket_name)

def get_orders_with_bellhops(**kwargs):
    sql = """select ord.reservation_start,ord.number, string_agg(usr.first_name || ' ' || usr.last_name, ',')
           from reservations_order ord join compensation_compensationselection cs on ord.id = cs.order_id
           join hophr_bellhopassignment bha on cs.id = bha.compensation_selection_id
           join hophr_bellhopprofile bhp on bha.bellhop_id = bhp.id 
           join auth_user usr on bhp.user_id = usr.id 
           group by ord.number,ord.reservation_start
           order by ord.reservation_start;"""
    sql_records = postgres_call(sql)
    records = []
    for _r,r in enumerate(sql_records):
        try:
            names = r[2].encode('UTF-8')
            records.append([r[0].strftime('%Y-%m-%d'),r[1],":".join(names.split(','))])
        except:
            continue
    with open("/tmp/records.csv", "wb") as f:
        writer = csv.writer(f)
        writer.writerows(records)

def store_orders_with_bellhops(**kwargs):
    file_ = '/tmp/records.csv'
    s3_call(
        file_=file_,
        bucket_name='naveen-airflow')
    
def transfer_orders_with_bellhops(**kwargs):
    drop_table = "drop table order_assignments;"
    create_table = "create table order_assignments(reserve_date varchar(50), ord_number varchar(50), bellhops varchar(5000));"
    load_data = "copy order_assignments from 's3://naveen-airflow/latest' credentials 'aws_access_key_id=" + aws_key + ";aws_secret_access_key=" + aws_pass + "' delimiter ',';"
    check_table = "select exists (select 1 from information_schema.tables where table_name='order_assignments');" 
    #sqls = [drop_table, create_table, load_data]
    table_exists = redshift_call(check_table,'get')[0][0]
    print "table_exists:", table_exists
    if table_exists:
        sqls = [drop_table, create_table, load_data]
        for i in sqls:
            redshift_call(i)
    else:
        sqls = [create_table, load_data]
        for i in sqls:
            redshift_call(i)

postgres_to_local_csv = PythonOperator(
    task_id='postgres_to_local_csv',
    provide_context=True,
    python_callable=get_orders_with_bellhops,
    dag=dag)

local_csv_to_s3 = PythonOperator(
    task_id='local_csv_to_s3',
    provide_context=True,
    python_callable=store_orders_with_bellhops,
    dag=dag)

s3_to_redshift = PythonOperator(
    task_id='s3_to_redshift',
    provide_context=True,
    python_callable=transfer_orders_with_bellhops,
    dag=dag) 

local_csv_to_s3.set_upstream(postgres_to_local_csv)
s3_to_redshift.set_upstream(local_csv_to_s3)
    
