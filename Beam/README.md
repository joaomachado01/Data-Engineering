# Enablers Availability: Beam / Dataflow Flex Template (V5)

Apache Beam pipeline (Python) that reads JSON log entries from Cloud Storage, extracts a few fields and appends them to a BigQuery table. It is packaged as a **Dataflow Flex Template**, so it can be launched with a single `gcloud` command.

## Pipeline overview

```
GCS (JSON lines) ──> ReadFromText ──> ExtractFields (ParDo) ──> WriteToBigQuery (append)
```

1. **Read**: reads the input file(s) from GCS, one JSON document per line.
2. **Process**: `ExtractFields` parses each line and builds a row with:

   | Column | Source |
   |---|---|
   | `ingestion_time` | Current timestamp (`%Y-%m-%d %H:%M:%S`) |
   | `response_enabler` | `labels.response_enabler` |
   | `response_healthcheck` | `labels.response_healthcheck` |
   | `response_status` | `labels.response_status` |
   | `response_time` | `labels.response_time` |

   Missing labels default to an empty string.
3. **Write**: appends the rows to BigQuery (`WRITE_APPEND`).

## Files

| File | Purpose |
|---|---|
| `my_pipeline.py` | Beam pipeline code |
| `Dockerfile` | Builds the image used by the Flex Template |
| `metadata.json` | Flex Template metadata (parameters `input` and `table`). **Not** copied into the image |
| `json_data.json` | Sample input (one JSON log entry per line) |
| `README.md` | This file |

## Pipeline parameters

| Parameter | Required | Description | Example |
|---|---|---|---|
| `--input` | Yes | GCS path of the JSON input file | `gs://<BUCKET>/json_data.json` |
| `--table` | Yes | BigQuery output table | `<PROJECT_ID>:<DATASET>.<TABLE>` |

Any other arguments (e.g. `--runner`, `--region`, `--max_num_workers`) are passed through to Beam as pipeline options.

## Prerequisites

- A GCP project with the Dataflow, Cloud Build, Artifact Registry, Cloud Storage and BigQuery APIs enabled.
- `gcloud` CLI authenticated with permissions to build images and launch Dataflow jobs.
- A Dataflow worker service account with access to the GCS buckets and the BigQuery dataset.
- The BigQuery table **must already exist** with the schema below. The pipeline does not pass a schema to `WriteToBigQuery`, so `CREATE_IF_NEEDED` cannot create it.

  | Column | Type |
  |---|---|
  | `ingestion_time` | TIMESTAMP (or DATETIME / STRING) |
  | `response_enabler` | STRING |
  | `response_healthcheck` | STRING |
  | `response_status` | STRING |
  | `response_time` | STRING |

> No credentials are stored in this folder. Authentication relies on your `gcloud` login for builds and on the worker service account at runtime. Never add service account key files to this folder.

## Deployment steps

Replace the placeholders with your own values (`<PROJECT_ID>`, `<BUCKET>`, `<TEMP_BUCKET>`, `<REGION>`, `<GAR_REPO>`, `<DATASET>`, `<TABLE>`).

### 1. Create the Cloud Storage bucket

Create a bucket with three folders/areas:

- temp files (or a separate temp bucket, as used in step 5)
- input/output files
- Flex Template storage

### 2. Create an Artifact Registry repository

Create a Docker repository in Artifact Registry to hold the pipeline images.

### 3. Build the image

```bash
export TAG=`date +%Y%m%d-%H%M%S`
export SDK_CONTAINER_IMAGE="<REGION>-docker.pkg.dev/<PROJECT_ID>/<GAR_REPO>/my_base_image:$TAG"

gcloud builds submit . --tag $SDK_CONTAINER_IMAGE --project <PROJECT_ID>
```

Notes:

- `metadata.json` **must not** be part of the image build.
- `gcloud builds submit .` uploads the entire folder to Cloud Build. Use a `.gcloudignore` so only the needed files are sent:

  ```
  *
  !Dockerfile
  !my_pipeline.py
  ```

- The Dockerfile must be named `Dockerfile` (no extension) for `gcloud builds submit` to pick it up.

### 4. Build the Flex Template

```bash
gcloud dataflow flex-template build gs://<BUCKET>/enablers-availability-template.json \
    --image $SDK_CONTAINER_IMAGE \
    --sdk-language "PYTHON" \
    --metadata-file=metadata.json \
    --project <PROJECT_ID>
```

`FLEX_TEMPLATE_PYTHON_PY_FILE` is already defined in the Dockerfile, so it is not passed as a parameter here.

### 5. Run the Flex Template

```bash
gcloud dataflow flex-template run "enabler-availability-flex-job-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "gs://<BUCKET>/enablers-availability-template.json" \
    --region <REGION> \
    --staging-location "gs://<TEMP_BUCKET>" \
    --parameters input="gs://<BUCKET>/json_data.json" \
    --parameters table="<PROJECT_ID>:<DATASET>.<TABLE>" \
    --project "<PROJECT_ID>" \
    --max-workers=5
```

## Running locally (quick test)

```bash
python my_pipeline.py \
    --input json_data.json \
    --table <PROJECT_ID>:<DATASET>.<TABLE> \
    --runner DirectRunner \
    --temp_location gs://<TEMP_BUCKET>/tmp
```