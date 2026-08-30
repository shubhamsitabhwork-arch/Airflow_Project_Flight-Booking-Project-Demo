# Airflow DAG Explanation: `airflow_job.py`

This script serves as the orchestrator for the Flight Booking data pipeline. It monitors Google Cloud Storage for incoming raw data using a sensor, dynamically extracts environment-specific configurations from Airflow Variables, and submits a serverless PySpark batch job to Google Cloud Dataproc without provisioning permanent clusters.

---

```python
from datetime import datetime, timedelta
import uuid  # Import UUID for unique batch IDs
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable
What it does: Imports all essential Python and Airflow dependencies:  datetime and timedelta: Used to configure scheduling and task retry delays.  uuid: Generates a unique 8-character string for every run so Dataproc batch job IDs never clash.  DAG: The foundational Airflow class used to define the workflow.  DataprocCreateBatchOperator: The specialized Google Cloud provider operator that submits jobs directly to Dataproc Serverless (Spark on GCP without managing virtual machine clusters).  GCSObjectExistenceSensor: A reactive sensor that pauses the pipeline until the specified raw CSV file lands in Google Cloud Storage.  Variable: Airflow's built-in key-value store module used to retrieve environment configs securely.  Python# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 13),
}
What it does: Sets baseline rules for every task in this DAG:  owner: Identifies the administrator owning the pipeline.  depends_on_past: False: Allows tasks to run independently of whether yesterday's run succeeded.  retries: 1 & retry_delay: timedelta(minutes=5): If a network or cloud execution error occurs, Airflow waits 5 minutes before attempting one retry.  start_date: The earliest historical anchor date from which the DAG is valid.  Python# Define the DAG
with DAG(
    dag_id="flight_booking_dataproc_bq_dag",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or on-demand
    catchup=False,
) as dag:
What it does: Instantiates the DAG using Python's context manager syntax (with ... as dag:).  dag_id: The unique identifier displayed in the Airflow Web UI.  schedule_interval=None: Configures the pipeline to run on-demand (or when triggered by CI/CD / file sensors) rather than on a recurring clock timer.  catchup=False: Prevents Airflow from triggering backfilled DAG runs for all missed historical dates between start_date and today.  Python    # Fetch environment variables
    env = Variable.get("env", default_var="dev")
    gcs_bucket = Variable.get("gcs_bucket", default_var="airflow-test-projects-gds-dev")
    bq_project = Variable.get("bq_project", default_var="project-d1694a9a-9dde-4e3c-974")
    bq_dataset = Variable.get("bq_dataset", default_var=f"flight_data_{env}")
    tables = Variable.get("tables", deserialize_json=True)
What it does: Dynamically reads configuration parameters imported into the Airflow metadata database:  env: Identifies the execution tier (dev or prod).  gcs_bucket: The Cloud Storage bucket housing the dataset and Python scripts.  bq_project: The GCP Project ID hosting the target BigQuery tables.  bq_dataset: Dynamically sets the target BigQuery dataset name (e.g., flight_data_dev or flight_data_prod) based on the environment.  tables: Deserializes the nested JSON string from Airflow Variables into a native Python dictionary containing target table names.  Python    # Extract table names from the 'tables' variable
    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]
What it does: Unpacks the specific BigQuery table names from the tables dictionary for clean reference when passing arguments to PySpark.  Python    # Generate a unique batch ID using UUID
    job_batch_id = f"flight-booking-batch-{env}-{str(uuid.uuid4())[:8]}"  # Shortened UUID for brevity
What it does: Generates a unique string identifier (e.g., flight-booking-batch-dev-19405b84). GCP Dataproc Serverless strictly requires every submitted batch job to have a globally unique batch ID within the region.  Python    # # Task 1: File Sensor for GCS
    file_sensor = GCSObjectExistenceSensor(
        task_id="check_file_arrival",
        bucket=gcs_bucket,
        object=f"flight-booking-analysis/source-{env}/flight_booking.csv",  # Full file path in GCS
        google_cloud_conn_id="google_cloud_default",  # GCP connection
        timeout=300,  # Timeout in seconds
        poke_interval=30,  # Time between checks
        mode="poke",  # Blocking mode
    )
What it does: Defines Task 1 of the pipeline.  Monitors the designated GCS path (flight-booking-analysis/source-{env}/flight_booking.csv).  poke_interval=30: Checks GCS every 30 seconds to see if the file exists.  timeout=300: If the file does not appear within 5 minutes (300 seconds), the task times out and fails gracefully.  google_cloud_conn_id: References Airflow's built-in GCP service connection.  

Python    # Task 2: Submit PySpark job to Dataproc Serverless
    batch_details = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{gcs_bucket}/flight-booking-analysis/spark-job/spark_transformation_job.py",  # Main Python file
            "python_file_uris": [],  # Python WHL files
            "jar_file_uris": [],  # JAR files
            "args": [
                f"--env={env}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}",
            ]
        },
        "runtime_config": {
            "version": "2.2",  # Specify Dataproc version (if needed)
        },
        "environment_config": {
            "execution_config": {
                "service_account": "1060029794842-compute@developer.gserviceaccount.com",
                "network_uri": f"projects/{bq_project}/global/networks/default",
                "subnetwork_uri": f"projects/{bq_project}/regions/us-central1/subnetworks/default",
            }
        },
    }
What it does: Builds the JSON configuration payload required by the Dataproc Serverless API:  main_python_file_uri: Points to the executable PySpark transformation script stored in GCS.  args: Feeds command-line arguments into the PySpark script so the transformation knows which project, dataset, and table names to write to.  runtime_config: Specifies Dataproc Runtime version 2.2 (providing Spark 3.5+ and built-in BigQuery connectors).  environment_config: Dictates the execution IAM service account and VPC subnetwork for networking and security compliance.  Python    pyspark_task = DataprocCreateBatchOperator(
        task_id="run_spark_job_on_dataproc_serverless",
        batch=batch_details,
        batch_id=job_batch_id,
        project_id=bq_project,
        region="us-central1",
        gcp_conn_id="google_cloud_default",
    )
What it does: Defines Task 2 of the pipeline. It transmits the batch_details payload to GCP and monitors the serverless Spark job until completion.  Python    # Task Dependencies
    file_sensor >> pyspark_task
What it does: Sets the task execution order using the bitshift operator (>>)[cite: 14]. It guarantees that the Spark batch job is submitted only after the GCS sensor confirms that the raw data file has landed[cite: 14].