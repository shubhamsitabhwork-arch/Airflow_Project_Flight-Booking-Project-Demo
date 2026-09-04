
### File 3: `CODE_EXPLANATION_cicd.md`

```markdown
# GitHub Actions CI/CD Explanation: `cicd.yaml`

This workflow file automates the continuous integration and deployment (CI/CD) for the flight booking data engineering project. It triggers on Git pushes to the `dev` and `main` branches and synchronizes Airflow DAGs, PySpark scripts, and environment variables with the respective GCP Cloud Composer environments.

---

```yaml
name: Flight Booking CICD

on:
  push:
    branches:
      - dev
      - main
What it does: Defines the workflow name and trigger conditions. The pipeline automatically executes whenever new commits are pushed to either the dev or main branches.  YAMLjobs:
  upload-to-dev:
    if: github.ref == 'refs/heads/dev'
    runs-on: ubuntu-latest
What it does: Defines the Development deployment job.  if: github.ref == 'refs/heads/dev': Conditional check ensuring this job only executes when commits are made to the dev branch.  runs-on: ubuntu-latest: Provisions a clean Ubuntu runner VM on GitHub's cloud.  YAML    steps:
      # Checkout the repository
      - name: Checkout Code
        uses: actions/checkout@v3
What it does: Clones the repository code onto the runner VM so subsequent steps can access the project files.  YAML      # Authenticate to GCP
      - name: Authenticate to GCP
        uses: google-github-actions/auth@v1
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY }}

      # Setup Google Cloud SDK
      - name: Setup Google Cloud SDK
        uses: google-github-actions/setup-gcloud@v1
        with:
          project_id: ${{ secrets.GCP_PROJECT_ID }}
What it does: Authenticates the runner against Google Cloud:  Uses GitHub Secrets (GCP_SA_KEY and GCP_PROJECT_ID) to log in via a dedicated GCP Service Account.  Installs and configures the gcloud and gsutil command-line tools.  YAML      # Upload `variables.json` to Composer bucket
      - name: Upload Variables JSON to GCS
        run: |
          gsutil cp 04_Assets/variables/dev/variables.json gs://us-central1-airflow-dev-220334f5-bucket/data/dev/variables.json

      # Import Variables into Airflow-DEV
      - name: Import Variables into Airflow-DEV
        run: |
          gcloud composer environments run airflow-dev \
            --location us-central1 \
            variables import -- /home/airflow/gcs/data/dev/variables.json
What it does:Copies variables.json from the local 04_Assets/variables/dev/ folder to the Cloud Composer GCS storage bucket.  Executes the gcloud composer environments run command to import those variables into the airflow-dev metadata database.  YAML      # Sync Spark job to GCS
      - name: Upload Spark Job to GCS
        run: |
          gsutil cp 04_Assets/spark_transformation_job.py gs://airflow-test-projects-gds-dev/flight-booking-analysis/spark-job/

      # Sync Airflow DAG to Airflow DEV Composer
      - name: Upload Airflow DAG to DEV Environment (Dag Folder)
        run: |
          gcloud composer environments storage dags import \
            --environment airflow-dev \
            --location us-central1 \
            --source 04_Assets/airflow_job.py
What it does:Uploads the PySpark script to the centralized GCS script storage directory.  Syncs airflow_job.py directly into the dags/ folder of the airflow-dev Composer environment.  YAML  upload-to-prod:
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
What it does: Defines the Production deployment job[cite: 17]. It triggers exclusively when code is merged into the main branch, applying the same automated authentication, variable import, and DAG sync steps targeted at the airflow-prod environment[cite: 17].