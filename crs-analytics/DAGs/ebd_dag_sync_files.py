from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.gcs import GoogleCloudStorageListOperator

default_args = {
    'depends_on_past': False
}

GCP_CS_BUCKET = ""
GCP_CS_PREFIX = ""
GCP_CS_DELIMITER = ".csv"

with DAG(
    'ebd-crs-prod-housekeeping',
    default_args=default_args,
    description='DAG to run prophet script on schedule',
    schedule_interval=None,
    start_date = days_ago(2)
) as dag:

    GCS_Files = GoogleCloudStorageListOperator(
        task_id='List_Files',
        bucket=GCP_CS_BUCKET,
        prefix=GCP_CS_PREFIX,
        delimiter=GCP_CS_DELIMITER,
    )