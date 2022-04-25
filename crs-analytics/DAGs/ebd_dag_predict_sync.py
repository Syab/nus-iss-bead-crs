from airflow import DAG
from airflow.utils.helpers import chain
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'depends_on_past': False
}

PROJECT_ID = 'syab-node-projects'
CLUSTER_NAME_PS = 'crs-pyspark-prod-cluster'
REGION_PS = 'asia-east1'
CLUSTER_NAME_HD = 'crs-hadoop-prod-cluster'
REGION_HD = 'asia-southeast1'
PYSPARK_URI = 'gs://ebd-crs-analytics/cpklot_prediction_gcp.py'

pip_path = "gs://goog-dataproc-initialization-actions-asia-east1/python/pip-install.sh"

CLUSTER_GENERATOR_CONFIG_PS = ClusterGenerator(
    project_id="syab-node-projects",
    master_machine_type="c2-standard-4",
    num_workers=0,
    image_version="2.0-rocky8",
    init_actions_uris=[pip_path],
    metadata={'PIP_PACKAGES': 'pandas pystan==2.19.1.1 prophet==1.0.1 numpy py4j pyarrow setuptools findspark boto3 google-cloud-storage'},
).make()

CLUSTER_GENERATOR_CONFIG_HD = ClusterGenerator(
    project_id="syab-node-projects",
    master_machine_type="n1-standard-2",
    master_disk_size=30,
    worker_machine_type="n1-standard-2",
    worker_disk_size=30,
    num_workers=2,
    image_version="1.5-debian10",
    init_actions_uris=["gs://ebd-crs-analytics/init-script/init-aws.sh"],
).make()

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME_PS},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

HADOOP_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME_HD},
    "hadoop_job": {
        "main_jar_file_uri": "file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar",
        "args": "-update gs://ebd-crs-analytics/temp_res/*.csv s3a://ebd-demo/testprediction/".split(" ")
    },
}

bash_gsutil_delete_command = "gsutil -m rm gs://ebd-crs-analytics/temp_res/*"

with DAG(
    'ebd-crs-prod-workflow',
    default_args=default_args,
    description='DAG to run prophet script on schedule',
    schedule_interval=None,
    start_date=days_ago(2)
) as dag:

    create_ps_cluster = DataprocCreateClusterOperator(
        task_id="create_ps_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_GENERATOR_CONFIG_PS,
        region=REGION_PS,
        cluster_name=CLUSTER_NAME_PS,
    )

    create_hd_cluster = DataprocCreateClusterOperator(
        task_id="create_hd_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_GENERATOR_CONFIG_HD,
        cluster_name=CLUSTER_NAME_HD,
        region=REGION_HD
    )

    submit_ps_job = DataprocSubmitJobOperator(
        task_id="pyspark_prediction_task",
        job=PYSPARK_JOB,
        location=REGION_PS,
        project_id=PROJECT_ID
    )

    submit_hadoop_job = DataprocSubmitJobOperator(
        task_id="gcs_to_s3_copy",
        job=HADOOP_JOB,
        location=REGION_HD,
        project_id=PROJECT_ID,
    )

    bash_task = BashOperator(
        task_id="bash_task_delete_temp_files",
        bash_command=bash_gsutil_delete_command
    )

    delete_ps_cluster = DataprocDeleteClusterOperator(
        task_id="delete_ps_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME_PS,
        region=REGION_PS
    )

    delete_hd_cluster = DataprocDeleteClusterOperator(
        task_id="delete_hd_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME_HD,
        region=REGION_HD
    )

    # create_ps_cluster >> [submit_ps_job, create_hd_cluster] >> submit_hadoop_job >> bash_task >> [delete_ps_cluster, delete_hd_cluster]
    chain(
        create_ps_cluster,
        submit_ps_job,
        delete_ps_cluster,
        create_hd_cluster,
        submit_hadoop_job,
        bash_task,
        delete_hd_cluster
    )