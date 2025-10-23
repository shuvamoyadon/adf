from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
from airflow.utils.dates import days_ago

# -------------------------------------------------------------------------
# DAG Configuration
# -------------------------------------------------------------------------
PROJECT_ID = "your-gcp-project-id"
SOURCE_DATASET = "source_dataset"
SOURCE_TABLE = "source_table"
TARGET_DATASET = "target_dataset"
TARGET_TABLE = "target_table"
LOCATION = "US"  # Change if your BQ dataset is in another region
GCP_CONN_ID = "google_cloud_default"  # or your custom connection ID

# -------------------------------------------------------------------------
# DAG Definition
# -------------------------------------------------------------------------
with DAG(
    dag_id="bq_to_bq_transfer",
    description="Reads data from one BigQuery table and writes it to another BigQuery table.",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "etl", "example"],
) as dag:

    # ---------------------------------------------------------------------
    # Task: Copy data from source to target
    # ---------------------------------------------------------------------
    copy_bq_data = BigQueryInsertJobOperator(
        task_id="copy_bq_data",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": f"""
                    SELECT *
                    FROM `{PROJECT_ID}.{SOURCE_DATASET}.{SOURCE_TABLE}`
                """,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": TARGET_DATASET,
                    "tableId": TARGET_TABLE,
                },
                "writeDisposition": "WRITE_TRUNCATE",  
                "createDisposition": "CREATE_IF_NEEDED",
                "useLegacySql": False,  
            }
        },
        location=LOCATION,
    )

   
    copy_bq_data
