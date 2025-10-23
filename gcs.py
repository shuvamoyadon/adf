from airflow import DAG
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from datetime import datetime
from airflow.utils.dates import days_ago

# -------------------------------------------------------------------------
# DAG Configuration
# -------------------------------------------------------------------------
PROJECT_ID = "your-gcp-project-id"
DATASET_ID = "source_dataset"
TABLE_ID = "source_table"
BUCKET_NAME = "your-gcs-bucket-name"
EXPORT_PATH = f"bq_exports/{TABLE_ID}/data_{{{{ ds_nodash }}}}.csv"  # dynamic date folder
LOCATION = "US"
GCP_CONN_ID = "google_cloud_default"  # Change if using a custom GCP connection

# -------------------------------------------------------------------------
# DAG Definition
# -------------------------------------------------------------------------
with DAG(
    dag_id="bq_to_gcs_export",
    description="Export BigQuery table data to Google Cloud Storage in CSV format.",
    start_date=days_ago(1),
    schedule_interval=None,  # e.g. '0 3 * * *' for daily at 3AM
    catchup=False,
    tags=["bigquery", "gcs", "export"],
) as dag:

    # ---------------------------------------------------------------------
    # Task: Export BigQuery table → GCS
    # ---------------------------------------------------------------------
    export_bq_to_gcs = BigQueryToGCSOperator(
        task_id="export_bq_to_gcs",
        gcp_conn_id=GCP_CONN_ID,
        source_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
        destination_cloud_storage_uris=[f"gs://{BUCKET_NAME}/{EXPORT_PATH}"],
        export_format="CSV",  # Options: "CSV", "NEWLINE_DELIMITED_JSON", "PARQUET", "AVRO"
        print_header=True,
        field_delimiter=",",
    )

    export_bq_to_gcs
