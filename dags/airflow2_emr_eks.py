"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
from airflow import DAG
from airflow.hooks.base_hook import BaseHook

from emr_containers.operators.emr_containers import EMRContainerOperator

from airflow.utils.dates import days_ago
from datetime import timedelta
import os

DAG_ID = os.path.basename(__file__).replace(".py", "")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["test@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

JOB_DRIVER_ARG = {
    "sparkSubmitJobDriver": {
        "entryPoint": "local:///usr/lib/spark/examples/src/main/python/pi.py",
        "sparkSubmitParameters": "--conf spark.executors.instances=2 --conf spark.executors.memory=2G --conf spark.executor.cores=2 --conf spark.driver.cores=1",
    }
}

CONFIGURATION_OVERRIDES_ARG = {
    "monitoringConfiguration": {
        "cloudWatchMonitoringConfiguration": {
            "logGroupName": "/aws/eks/dacort-emr/eks-spark",
            "logStreamNamePrefix": "airflow",
        }
    }
}

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["emr_containers"],
) as dag:

    c = BaseHook.get_connection("emr_eks")
    cluster_args = c.extra_dejson

    job_starter = EMRContainerOperator(
        task_id="start_job",
        virtual_cluster_id=cluster_args.get('virtual_cluster_id'),
        execution_role_arn=cluster_args.get('job_role_arn'),
        release_label="emr-6.2.0-latest",
        job_driver=JOB_DRIVER_ARG,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        name="pi.py",
    )

# airflow tasks run -l -S /tmp/emr-eks.py emr-eks start_job 2020-05-06 -f -i
# Needed to set AWS_DEFAULT_REGION
# Now I need to grant access to the airflow role to run EMR Contianers
# May also need to clear the task during debugging:
# airflow tasks clear -S /tmp/emr-eks.py emr-eks