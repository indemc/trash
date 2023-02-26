from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from lib.rate_libs import create_necessary_tables, get_rates_from_source, transform_loaded_json

dag_id = "rate_loader_01"

DEFAULT_ARGS = {"owner": "airflow", "start_date": "2023-02-21"}

with DAG(
    dag_id,
    default_args=DEFAULT_ARGS,
    description="Sink rates into dwh",
    schedule_interval="0 */3 * * *",
    catchup=True,
    max_active_runs=1,
) as dag:


    create_necessary_tables_task = PythonOperator(
        dag=dag, 
        task_id=f"create_necessary_tables_id", 
        python_callable=create_necessary_tables
        )
        
    get_data_from_source_task = PythonOperator(
        dag=dag, 
        task_id=f"get_data_from_source_id", 
        python_callable=get_rates_from_source
        )
    transform_loaded_data_task = PythonOperator(
        dag=dag, 
        task_id=f"transform_loaded_data_id", 
        python_callable=transform_loaded_json
        )

    create_necessary_tables_task >> get_data_from_source_task >> transform_loaded_data_task
