from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

default_args = {
    'depends_on_past': False
}

CLUSTER_NAME = 'crs-airflow-prod-cluster'
REGION = 'asia-east1'
PROJECT_ID = 'syab-node-projects'
PYSPARK_URI = 'gs://ebd-crs-analytics/testingprophet.py'

path = "gs://goog-dataproc-initialization-actions-asia-east1/python/pip-install.sh"

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id="syab-node-projects",
    master_machine_type="c2-standard-4",
    num_workers=0,
    image_version="2.0-rocky8",
    init_actions_uris=[path],
    metadata={'PIP_PACKAGES': 'pandas pystan==2.19.1.1 prophet==1.0.1 numpy py4j pyarrow setuptools findspark boto3 google-cloud-storage'},
).make()

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

with DAG(
    'ebd-crs-prod-workflow',
    default_args=default_args,
    description='DAG to run prophet script on schedule',
    schedule_interval=None,
    start_date = days_ago(2)
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_GENERATOR_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_prophet_task",
        job=PYSPARK_JOB,
        location=REGION,
        project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION
    )

    create_cluster >> submit_job >> delete_cluster