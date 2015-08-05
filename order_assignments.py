from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
from airflow.hooks import PostgresHook, S3Hook
from datetime import datetime, timedelta
import csv

task_date = datetime.utcnow() - timedelta(1)

default_args = {
    'owner': 'naveen',
    'depends_on_past': False,
    'start_date': task_date
}

dag = DAG('get_orders_from_postgres_single', default_args=default_args)

def postgres_call(sql):
    pghook = PostgresHook(postgres_conn_id="naveen_ngrok_postgres")
    return pghook.get_records(sql)
    
def s3_call(file_, bucket_name):
    s3hook = S3Hook(s3_conn_id="naveen_s3")
    s3hook.load_file(
        filename=file_,
        key=task_date.strftime('latest'),
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

local_csv_to_s3.set_upstream(postgres_to_local_csv)

    
