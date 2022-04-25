from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator,DataprocSubmitJobOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterDeleteOperator
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator
from airflow.operators.bash_operator import BashOperator

PROJECT_ID = "syab-node-projects"
CLUSTER_NAME = "ebd-hadoop-cluster"
REGION = "asia-southeast1"
# path = "gs://goog-dataproc-initialization-actions-asia-east1/python/pip-install.sh"

# CLUSTER_CONFIG = {
#     "master_config": {
#       "num_instances": 1,
#       "machine_type_uri": "n1-standard-2",
#       "disk_config": {"boot_disk_size_gb": 200},
#       "image_version": "1.5-debian10"
#     },
#     "worker_config": {
#       "num_instances": 2,
#       "machine_type_uri": "n1-standard-2",
#       "image_version": "1.5-debian10",
#       "disk_config": {"boot_disk_size_gb": 200},
#     },
#     # "secondary_worker_config": {"num_instances": 5},
#     "initialization_actions": [{"executable_file": "gs://ebd-crs-analytics/init-script/init-aws.sh"}]
#   }

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id="syab-node-projects",
    master_machine_type="n1-standard-2",
    master_disk_size=30,
    worker_machine_type="n1-standard-2",
    worker_disk_size=30,
    num_workers=2,
    image_version="1.5-debian10",
    init_actions_uris=["gs://ebd-crs-analytics/init-script/init-aws.sh"],
).make()


HADOOP_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "hadoop_job": {
        "main_jar_file_uri": "file:///usr/lib/hadoop-mapreduce/hadoop-distcp.jar",
        "args": "-update gs://ebd-crs-analytics/testdata_all_location/*.csv s3a://ebd-demo/testprediction/".split(" ")
    },
}

bash_gsutil_delete_command = "gsutil -m rm gs://ebd-crs-analytics/testdata_all_location/*"


with DAG(
    'ebd-dag-file-transfer',
    description='DAG to transfer file from GCS to S3',
    start_date=days_ago(1),
    schedule_interval=None
) as dag:
    create_cluster = DataprocCreateClusterOperator(
            task_id="create_cluster",
            project_id=PROJECT_ID,
            cluster_config=CLUSTER_GENERATOR_CONFIG,
            cluster_name=CLUSTER_NAME,
            region=REGION
        )
    submit_hadoop_job = DataprocSubmitJobOperator(
            task_id="gcs_to_s3_copy",
            job=HADOOP_JOB,
            location=REGION,
            project_id=PROJECT_ID,
        )
    bash_task = BashOperator(
            task_id="bash_task_delete_temp_files",
            bash_command=bash_gsutil_delete_command
    )
    # delete_cluster = DataprocClusterDeleteOperator(
    #         task_id="delete_cluster",
    #         project_id=PROJECT_ID,
    #         cluster_name=CLUSTER_NAME,
    #         region=REGION,
    #         dag=dag
    # )

    create_cluster >> submit_hadoop_job >> bash_task

    # create_cluster >> submit_hadoop_job >> delete_cluster