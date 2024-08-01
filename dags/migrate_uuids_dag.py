from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.utils.dates import days_ago

from datetime import timedelta
from ala import cluster_setup, ala_config, ala_helper
from ala.ala_helper import step_bash_cmd, s3_cp, get_default_args

DAG_ID = 'migrate_uuid_WARN'

SPARK_STEPS = [
    s3_cp(f"a. Copy uuid export from s3", f"s3://{ala_config.S3_BUCKET_MIGRATION}/migration", f"hdfs:///migration"),
    step_bash_cmd("b. Migrate UUIDs", f" migrate-uuids-emr.sh"),
    step_bash_cmd("c. Upload UUIDs", f" s3-upload-migrated-uuids.sh {ala_config.S3_BUCKET_AVRO}")
]

with DAG(
    dag_id=DAG_ID,
    description="WARNING - This will delete all current identifiers in s3. Should not be run once ingesting new data. Imports uuids from biocache-store export.",
    default_args=get_default_args(),
    dagrun_timeout=timedelta(hours=5),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['emr', 'all-datasets']
) as dag:

    cluster_setup.run_large_emr(dag, SPARK_STEPS, "bootstrap-migration-actions.sh", cluster_size=1)