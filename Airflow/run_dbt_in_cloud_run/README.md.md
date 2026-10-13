# Airflow Cloud Run dbt Pipeline

This repository contains an Apache Airflow DAG (`dag_to_trigger_cloudrun_to_run_dbt`) designed to trigger Google Cloud Run jobs that execute dbt (data build tool) projects and tests asynchronously.

---

## 📌 Overview

The Airflow DAG automates the sequential execution of **dbt run** and **dbt test** commands across multiple sub-projects via the Google Cloud Run v2 REST API. 

### Architecture Flow
1. **Airflow Task Group Init**: Generates execution steps per sub-project (`firestore`, `audit`, etc.).
2. **Trigger dbt Run**: Sends an HTTP `POST` request to the Cloud Run API to trigger a job configured with `dbt run` overrides.
3. **Polling Job Completion**: Polls the Cloud Run API execution status every 10 seconds until completion.
4. **Trigger dbt Test**: Upon successful run completion, executes a second Cloud Run job configured with `dbt test` overrides.

---

## 📁 Repository Structure

```text
.
├── dags/
│   └── dag_to_trigger_cloudrun_to_run_dbt.py   # Airflow DAG definition
└── README.md                                   # Documentation
```

---

## ⚙️ Configuration & Variables

Before running this DAG in your environment, update or inject the following parameters:

| Variable | Description | Example / Default |
| :--- | :--- | :--- |
| `PROJECT_ID` | GCP Project ID hosting Cloud Run | `your-gcp-project-id` |
| `REGION` | GCP Region where Cloud Run Jobs reside | `europe-west1` |
| `SUB_PROJECTS` | Array of sub-projects to execute | `["firestore", "audit"]` |
| `CR_JOB_NAME_VAR_NAME` | Cloud Run job name template pattern | `gcr-xxx-PROJECT-d-europe-west1-dbt` |
| `SA_ASTRONOMER` | Service Account executing the tasks | `your-sa@your-project.iam.gserviceaccount.com` |

---

## 🔐 Prerequisites & Permissions

1. **Google Cloud Service Account Permissions:**
   The Airflow worker environment (e.g., Cloud Composer or Astronomer) must have Application Default Credentials (ADC) or a service account key configured with the following roles:
   * **Cloud Run Developer** (`roles/run.developer`) or **Cloud Run Admin** (`roles/run.admin`) to invoke jobs.
   * **Service Account User** (`roles/iam.serviceAccountUser`) if running under specific identity constraints.

2. **Required Python Packages:**
   Ensure the following packages are installed in your Airflow environment:
   * `apache-airflow-providers-google`
   * `google-auth`
   * `google-auth-oauthlib`
   * `requests`

---

## 🚀 Execution & Usage

1. Place the DAG file into your Airflow environment's `dags/` folder.
2. Verify that Application Default Credentials (ADC) are configured properly on your Airflow worker nodes.
3. Enable and trigger `dag_to_trigger_cloudrun_to_run_dbt` via the Airflow UI or CLI:

```bash
airflow dags trigger dag_to_trigger_cloudrun_to_run_dbt
```

---

## 🛑 Security Notice

* **No Hardcoded Credentials**: Do not hardcode service account keys, bearer tokens, or sensitive API secrets directly inside the DAG file. Use Airflow Connections or Environment Variables for managing environment specifics.
* **Authentication**: Authentication is handled dynamically using Google Application Default Credentials (`google.auth.default`).