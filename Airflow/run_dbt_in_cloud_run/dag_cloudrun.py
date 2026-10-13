from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import airflow.providers.google.cloud.operators.bigquery
import airflow.providers.google.cloud.operators.gcs
import airflow.providers.google.cloud.operators.functions
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import argparse
import json
import logging
import google.auth
from google.auth import compute_engine
from google.oauth2 import id_token
from google.auth.transport.requests import AuthorizedSession
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
import time
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

### Variables ###
SUB_PROJECTS = ["firestore", "audit"]
CR_JOB_NAME_VAR_NAME = 'gcr-xxx-PROJECT-d-europe-west1-dbt'
PROJECT_ID = 'xxx'
SA_ASTRONOMER = 'xxx@xxx.iam.gserviceaccount.com'
REGION = "europe-west1"
ARGS = ["run", "test"]
DATA_DIC = {
    "overrides": {
        "containerOverrides": [
            {
                "args": ARGS[0]
            }
        ]
    }
}
DATA_DIC_TEST = {
    "overrides": {
        "containerOverrides": [
            {
                "args": ARGS[1]
            }
        ]
    }
}

### Functions ###
def trigger_cloud_run_job(url, data, url_status):
        """
            Triggers an HTTP cloud run job.
            Args:
                url (str): URL of the cloudrun job.
                data (dict): Data to pass to the cfuntion.
        """

        logging.info(f"Triggering Cloud Run job - {url}")

        creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])

        # Use the credentials to obtain an access token
        creds.refresh(google.auth.transport.requests.Request())
        access_token = creds.token

        resp = requests.post(
            url,
            data=json.dumps(data),
            headers={'Content-Type': 'application/json','Authorization': f'Bearer {access_token}'},
            timeout=540
        )

        logging.info(f"Status Code: {str(resp.status_code)}")
        logging.info(f"Response: {str(resp.text)}")

        assert resp.status_code == 200, "Response code was different from 200 OK."

        '''
            Get job status until its OK
        '''

        done = False
        while done != True:
            resp_job = requests.get(
                f"{url_status}/{resp.json().get('name')}",
                headers={'Content-Type': 'application/json','Authorization': f'Bearer {access_token}'},
                timeout=540
            )

            logging.info(f"Status Code: {str(resp_job.status_code)}")
            resp_json = resp_job.json()

            if resp_json.get('done') == True:
                done = True
                assert "error" not in resp_json, resp_json.get('error').get('message')
                return resp_json
            logging.info(f"Job still running...")
            time.sleep(10)


with DAG(
    dag_id='dag_to_trigger_cloudrun_to_run_dbt',
    description='Invoke Cloud Run job to run DBT project',
    start_date=datetime(2023, 12, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    start_task = EmptyOperator(
                task_id='start_task'
    )
    task_dict = {}
    for sub_project in SUB_PROJECTS:
        def process_project():
            with TaskGroup(group_id=f'{sub_project}_dbt_project') as task_group:

                start_task = EmptyOperator(
                    task_id='start_task'
                )

                trigger_cloud_run = PythonOperator(
                    task_id='trigger_cloud_run',
                    python_callable=trigger_cloud_run_job,
                    op_kwargs={"url": f"https://run.googleapis.com/v2/projects/{PROJECT_ID}/locations/{REGION}/jobs/{CR_JOB_NAME_VAR_NAME}:run".replace('PROJECT', sub_project),
                                "data": DATA_DIC,
                                "url_status": "https://run.googleapis.com/v2"
                            }
                )

                end_task = EmptyOperator(
                    task_id='end_task',
                    trigger_rule=TriggerRule.ALL_SUCCESS
                )

                start_task >> trigger_cloud_run >> end_task

            return task_group
        task_dict[sub_project] = process_project()

        def process_project_test():
            with TaskGroup(group_id=f'{sub_project}_dbt_project_test') as task_group:

                start_task = EmptyOperator(
                    task_id='start_task',
                    trigger_rule=TriggerRule.ALL_SUCCESS  # Set trigger rule
                )

                trigger_cloud_run = PythonOperator(
                    task_id='trigger_cloud_run',
                    python_callable=trigger_cloud_run_job,
                    op_kwargs={"url": f"https://run.googleapis.com/v2/projects/{PROJECT_ID}/locations/{REGION}/jobs/{CR_JOB_NAME_VAR_NAME}:run".replace('PROJECT', sub_project),
                                "data": DATA_DIC_TEST,
                                "url_status": "https://run.googleapis.com/v2"
                            }
                )

                end_task = EmptyOperator(
                    task_id='end_task',
                    trigger_rule=TriggerRule.ALL_DONE  # Set trigger rule
                )

                start_task >> trigger_cloud_run >> end_task

            return task_group
        task_dict[sub_project + '_test'] = process_project_test()

    # check_task_firestore = EmptyOperator(
    #             task_id='check_task_firestore',
    #             trigger_rule=TriggerRule.ALL_SUCCESS  # Set trigger rule
    # )

    # check_task_audit = EmptyOperator(
    #             task_id='check_task_audit',
    #             trigger_rule=TriggerRule.ALL_SUCCESS  # Set trigger rule
    # )

    end_task = EmptyOperator(
                task_id='end_task',
                trigger_rule=TriggerRule.ALL_SUCCESS  # Set trigger rule
    )

    # Define the dependencies using a loop
    for i in range(len(SUB_PROJECTS) -1):
        task_dict[SUB_PROJECTS[i]] >> task_dict[SUB_PROJECTS[i] + '_test']
        task_dict[SUB_PROJECTS[i + 1]] >> task_dict[SUB_PROJECTS[i + 1] + '_test']

    # Connect the start_task and end_task
    start_task >> task_dict[SUB_PROJECTS[0]] >> end_task
    start_task >> task_dict[SUB_PROJECTS[-1]] >> end_task
    task_dict[SUB_PROJECTS[0] + '_test'] >> end_task
    task_dict[SUB_PROJECTS[-1] + '_test'] >> end_task

    # task_dict[SUB_PROJECTS[0]] >> check_task_firestore
    # task_dict[SUB_PROJECTS[-1]] >> check_task_audit
    # check_task_firestore >> end_task
    # check_task_audit >> end_task