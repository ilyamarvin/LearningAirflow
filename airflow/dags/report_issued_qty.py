from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from clickhouse_driver import Client
import psycopg2
import json

default_args = {
    'owner': 'sereda',
    'start_date': datetime(2024, 7, 28)
}

dag = DAG(
    dag_id='report_issued_qty',
    default_args=default_args,
    schedule_interval='@daily',
    description='Даг для заполнения витрины на ПГ',
    catchup=False,
    max_active_runs=1
)


def connect_ch():
    with open('/opt/airflow/dags/credentials.json') as json_file:
        data = json.load(json_file)

    client = Client(data['clickhouse'][0]['host'],
                    user=data['clickhouse'][0]['user'],
                    password=data['clickhouse'][0]['password'],
                    port=data['clickhouse'][0]['port'],
                    verify=False,
                    settings={"numpy_columns": True, 'use_numpy': True},
                    compression=False)

    return client


def connect_pg():
    with open('/opt/airflow/dags/credentials.json') as json_file:
        data = json.load(json_file)

    client = psycopg2.connect(host=data['postgres'][0]['host'],
                                  user=data['postgres'][0]['user'],
                                  password=data['postgres'][0]['password'],
                                  port=data['postgres'][0]['port'],
                                  dbname=data['postgres'][0]['dbname'])

    return client

def main():
    main_table = "report.issued_qty"

    sql = f'''
        insert into {main_table}
            select toDate(issued_dt) dt_date
                , wh_id
                , count(shk_id) qty_issued
            from assembly_task_issued
            group by dt_date, wh_id
    '''

    client_ch = connect_ch()
    client_ch.execute(sql)
    print(f'Запись в витрину данных {main_table} прошла успешно!')

def import_pg():
    procedure_name = "issued_qty_importfromclick"
    main_table = "report.issued_qty"

    sql = f'''
        select now() dt_load
            , dt_date
            , wh_id
            , qty_issued
        from {main_table} final
    '''

    client_ch = connect_ch()
    df = client_ch.query_dataframe(sql)

    client_pg = connect_pg()
    cursor = client_pg.cursor()

    df = df.to_json(orient="records", date_format="iso", date_unit="s")
    cursor.execute(f"CALL sync.{procedure_name}(_src := '{df}')")
    client_pg.commit()

    print('Импорт данных прошел успешно')

    cursor.close()
    client_pg.close()


task_ch = PythonOperator(task_id='report_issued_qty_ch', python_callable=main, dag=dag)
task_pg = PythonOperator(task_id='report_issued_qty_pg', python_callable=import_pg, dag=dag)

task_ch >> task_pg