from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import pandas as pd
import json

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def extract():
    response = requests.get('https://restcountries.com/v3/all')
    return(response.text)

@task
def transform(response):
    countries_data = json.loads(response)
    transformed_countries = []

    for country in countries_data:
        country_info = {
            'name':country['name']['official'],
            'population' : country['population'],
            'area' : country['area']
        }
        transformed_countries.append(country_info)

    return transformed_countries

@task
def load(schema, table, records):
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
   CREATE TABLE {schema}.{table} (
   country varchar(32) primary key,
   population int,
   area float
);""")
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES ('{r[0]}', {r[1]}, {r[2]});"
            print(sql)
            cur.execute(sql)
            cur.execute("COMMIT;")
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise


with DAG(
    dag_id = 'countries',
    start_date = datetime(2024,11,13),
    catchup=False,
    schedule = '30 6 * * 6'
) as dag:
    
    schema = 'estella_hj_kim'
    table = 'country_info'

    records = transform(extract())
    load(schema, table, records)