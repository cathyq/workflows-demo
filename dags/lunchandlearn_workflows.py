from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator


cluster = {
    'spark_version': '9.1.x-scala2.12',
    'node_type_id': 'r3.xlarge',
    'aws_attributes': {'availability': 'ON_DEMAND'},
    'num_workers': 8
}

sql_query = [
    """
    CREATE OR REPLACE TABLE workflows_demo.click_sales
    AS (SELECT transaction_id
                , workflows_demo.sales.user_id
                , product_id
                , product_name
                , amount
                , transaction_timestamp
                , click_id
                , user_name
                , click_timestamps
        FROM workflows_demo.sales
        JOIN workflows_demo.clicks
        ON workflows_demo.sales.user_id = workflows_demo.clicks.user_id)
    """
]

sql_query_complete = [
    """
    CREATE OR REPLACE TABLE workflows_demo.complete_data
    AS (SELECT transaction_id
                , workflows_demo.click_sales.user_id
                , product_id
                , product_name
                , amount
                , transaction_timestamp
                , click_id
                , user_name
                , click_timestamps
        FROM workflows_demo.click_sales
        LEFT JOIN workflows_demo.campaign
        WHERE workflows_demo.click_sales.user_id = workflows_demo.campaign.user_id)
    """
]

def download_data_api(s):
    return s + "data api"

def notify_data_readiness(s) -> str:
    return s + "data readiness"


default_args = {
    'owner': 'cathy',
    'start_date': datetime(2022, 10, 10),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


with DAG('cathy_workflows', default_args=default_args, schedule_interval="0 0 * * *", catchup=False) as dag:

   
    
    sales_data = DatabricksSubmitRunOperator(task_id="sales",
                                            new_cluster=cluster,
                                            databricks_conn_id="databricks",
                                            notebook_task={'notebook_path': '/Users/cathy.zdravevski@databricks.com/sales'})

    clicks_data = DatabricksSubmitRunOperator(task_id="clicks",
                                            new_cluster=cluster,
                                            databricks_conn_id="databricks",
                                            notebook_task={'notebook_path': '/Users/cathy.zdravevski@databricks.com/clicks'})

    join_data = DatabricksSqlOperator(task_id="join",
                                    sql=sql_query,
                                    databricks_conn_id="databricks",
                                    sql_endpoint_name="Shared Endpoint")

    notify_data = PythonOperator(task_id="notify_data",
                                python_callable=notify_data_readiness,
                                op_kwargs = {"s" : "notify"},
                                dag=dag)

    download_data = PythonOperator(task_id="download_data_api",
                                python_callable=download_data_api,
                                op_kwargs = {"s" : "downloads"},
                                dag=dag)

    join_other_data = DatabricksSqlOperator(task_id="join_other_data",
                                            sql=sql_query_complete,
                                            databricks_conn_id="databricks",
                                            sql_endpoint_name="Shared Endpoint")


    sales_data >> join_data
    clicks_data >> join_data
    join_data >> join_other_data
    download_data >> join_other_data
    join_other_data >> notify_data
