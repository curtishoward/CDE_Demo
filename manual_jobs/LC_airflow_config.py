from dateutil import parser
from datetime import datetime, timedelta
from datetime import timezone
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator


default_args = {
    'owner': 'pauldefusco',
    'retry_delay': timedelta(seconds=5),
    'start_date': datetime(2021,1,1,1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    'LC_customer_scoring',
    default_args=default_args,
    schedule_interval='0 0 12 * *',
    catchup=False,
    is_paused_upon_creation=False
)


start = DummyOperator(task_id='start', dag=dag)


data_exploration = CDEJobRunOperator(
    task_id='data_exploration',
    retries=3,
    dag=dag,
    job_name='LC_data_exploration'
)

kpi_reports = CDEJobRunOperator(
    task_id='KPI_reports',
    dag=dag,
    job_name='LC_KPI_reporting'
)

customer_scoring = CDEJobRunOperator(
    task_id='ML_Scoring',
    dag=dag,
    job_name='LC_ml_scoring'
)

end = DummyOperator(task_id='end', dag=dag)


start >> data_exploration >> kpi_reports >> customer_scoring >> end
