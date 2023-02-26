



import datetime
from clickhouse_driver import Client
import pandas as pd
import requests
import json


def _db_connect(need_df: int):
    client = ''
    if need_df == 1:
        client = Client(host='clickhouse-server', settings={'use_numpy': True})
    else:
        client = Client(host='clickhouse-server')
        
    return client

def create_necessary_tables():
    client = _db_connect(0)
    client.execute("CREATE DATABASE IF NOT EXISTS test")
    client.execute("CREATE TABLE IF NOT EXISTS test.rates_raw(modified_date DateTime('Asia/Istanbul'), report_date Date, resp String) Engine = MergeTree Order By report_date")
    client.execute("CREATE TABLE IF NOT EXISTS test.rates(relation String, dt Date, rate Float) Engine = MergeTree Order By dt")

def get_rates_from_source(ti):
    client = _db_connect(0)
    url = 'https://api.exchangerate.host/latest?base=BTC&symbols=USD'
    cur_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ti.xcom_push(key='run_timex', value=cur_date)
    response = requests.get(url)
    data = response.json()
    report_date = data['date']
    data = json.dumps(data)
    client.execute(f"INSERT INTO test.rates_raw  (modified_date, report_date, resp) VALUES ('{cur_date}', '{report_date}', '{data}')")


def transform_loaded_json(ti):
    received_value = ti.xcom_pull(task_ids="get_data_from_source_id", key='run_timex')
    client = _db_connect(1)
    result = client.execute(f"select resp from test.rates_raw where modified_date = '{received_value}'")
    data = json.loads(result[0][0])
    print("============================ STEPP::04 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    del data['motd']
    del data['success']
    data = pd.DataFrame(data)
    print("============================ STEPP::05 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    data.reset_index(inplace=True)
    data["relation"] = data["base"] + "/" + data["index"]
    data.rename(columns={'rates': 'rate', 'date': 'dt'}, inplace=True)
    data = data[["relation", "dt", "rate"]]
    client.insert_dataframe(f'INSERT INTO test.rates VALUES', data)
    # result = client.execute("select * from test.rates")
    # print(report_date, self.cur_date, data)



