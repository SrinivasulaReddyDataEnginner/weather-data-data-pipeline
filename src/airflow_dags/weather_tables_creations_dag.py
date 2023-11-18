"""
Partner reporting comparison price Hudi Table boostrap job.
"""

import os

from airflow import models
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocInstantiateWorkflowTemplateOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.models import Variable
from datetime import datetime, timedelta

EMPTY_STRING = ""
PROJECT_ID = Variable.get("GCP_PROJECT_ID", EMPTY_STRING)
CLUSTER_NAME = Variable.get("GCP_DATAPROC_CLUSTER_NAME", "parpal-comparison-price-bootstrap")
REGION = Variable.get("GCP_LOCATION", "us-central1")
SUBNETWORK_URI = Variable.get("GCP_SUBNETWORK_URI", "projects/shared-vpc-admin/regions/us-central1/subnetworks/prod-us-central1-01")
GCP_SERVICE_ACCOUNT = Variable.get("PARPAL_GCP_SERVICE_ACCOUNT", EMPTY_STRING)
IMAGE_URI = "projects/wmt-pcloud-trusted-images/global/images/family/wmt-dataproc-custom-2-0"
MACHINE_TYPE = "n2-standard-16"


PYTHON_SCRIPT_PATH = 'gs://weather-report/binaries/python/weather_table_creation.py'


# Dataproc cluster definition

CLUSTER_CONFIG = {
    "gce_cluster_config": {
        "subnetwork_uri": SUBNETWORK_URI,
        "internal_ip_only": True,
        "service_account" : GCP_SERVICE_ACCOUNT
    },
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": MACHINE_TYPE,
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
        "image_uri" : IMAGE_URI
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": MACHINE_TYPE,
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
        "image_uri" : IMAGE_URI
    },
    "secondary_worker_config": {
        "num_instances": 2,
        "machine_type_uri": MACHINE_TYPE,
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
        "image_uri" : IMAGE_URI
    },
    "software_config": {
        "properties": {
            "dataproc:dataproc.allow.zero.workers": "true",
            "yarn:yarn.resourcemanager.scheduler.class": "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler",
            "dataproc:dataproc.logging.stackdriver.job.driver.enable": "true",
            "dataproc:dataproc.logging.stackdriver.enable": "true",
            "dataproc:jobs.file-backed-output.enable": "true",
            "dataproc:dataproc.logging.stackdriver.job.yarn.container.enable": "true",
            "yarn:yarn.resourcemanager.webapp.ui-actions.enabled": "true",
            "yarn:yarn.resourcemanager.am.max-attempts": "6",
            "mapred:mapreduce.map.maxattempts": "12",
            "mapred:mapreduce.reduce.maxattempts": "12",
            "spark:spark.task.maxFailures": "12",
            "spark:spark.stage.maxConsecutiveAttempts": "12"
        }
    },
    "endpoint_config" : {
       "enable_http_port_access" : True
    },
    "lifecycle_config": {
        "idle_delete_ttl": {"seconds": 15*60},
        "auto_delete_ttl": {"seconds": 480*60},
    },
}


default_args = {
    'owner': 'Weather Report Pipeline',
    'depends_on_past': False,
    'email': ['srinivasulareddy2172@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

def execute_python_script(**kwargs):
    # Import required modules and execute your Python script
    import subprocess
    subprocess.run(["python", PYTHON_SCRIPT_PATH])



with models.DAG("weather-table-creation-dag", start_date=datetime(2023, 11, 18), schedule_interval= None, catchup = False) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        retries = 3,
        retry_delay = timedelta(minutes=5)
    )

    # First task is to query data from the openweather.org
    weather_tables_creation_task = PythonOperator(
        task_id='weather_tables_creation',
        python_callable=execute_python_script,
        provide_context=True,
        dag=dag
    )


    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", trigger_rule='all_done', project_id=PROJECT_ID, cluster_name=CLUSTER_NAME, region=REGION, retries = 3, retry_delay = timedelta(minutes=5)
    )

    create_cluster >> weather_tables_creation_task >> delete_cluster
