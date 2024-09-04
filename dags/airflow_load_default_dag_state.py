from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import DagModel
from airflow.settings import Session

# The list of DAGs that should remain unpaused
UNPAUSED_DAGS = [
    'Airflow_load_default_dag_state',
    'Airflow_load_variables',
    'Assertions-Sync',
    'Full_index_to_solr',
    'Ingest_large_datasets',
    'Ingest_small_datasets',
    'Load_dataset',
    'Preingest_datasets'
]

# Define the DAG
dag = DAG(
    'Airflow_load_default_dag_state',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(1),
    },
    description='A DAG to pause all DAGs and unpause specific DAGs',
    schedule_interval=None,
)

def airflow_load_default_dag_state():
    # Start a new session
    session = Session()

    # Pause all DAGs
    session.query(DagModel).update({DagModel.is_paused: True}, synchronize_session=False)

    # Unpause specific DAGs
    if UNPAUSED_DAGS:
        session.query(DagModel).filter(DagModel.dag_id.in_(UNPAUSED_DAGS)).update({DagModel.is_paused: False}, synchronize_session=False)

    session.commit()

# Define the PythonOperator to pause all DAGs and unpause specific ones
airflow_load_default_dag_state_task = PythonOperator(
    task_id='airflow_load_default_dag_state_task',
    python_callable=airflow_load_default_dag_state,
    dag=dag,
)

# Set the task in the DAG
airflow_load_default_dag_state_task
