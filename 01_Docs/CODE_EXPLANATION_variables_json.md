
### File 4: `CODE_EXPLANATION_variables_json.md`

```markdown
# Configuration Explanation: `variables.json`

This JSON file defines the environment configuration parameters imported into Apache Airflow. It decouples environment-specific settings (such as project IDs, dataset targets, and table names) from the pipeline code.

---

```json
{
    "env": "prod",
    "gcs_bucket": "airflow-test-projects-gds-dev",
    "bq_project": "project-d1694a9a-9dde-4e3c-974",
    "bq_dataset": "flight_data_prod",
    "tables": {
      "transformed_table": "transformed_flight_data_prod",
      "route_insights_table": "route_insights_prod",
      "origin_insights_table": "origin_insights_prod"
    }
}
"env": Defines the deployment tier ("dev" or "prod"). This controls dynamic path resolution across the entire pipeline[cite: 14, 15].  "gcs_bucket": The Google Cloud Storage bucket where source CSV files and PySpark scripts reside.  "bq_project": The Google Cloud Project ID hosting the target BigQuery instance[cite: 14, 16]."bq_dataset": The BigQuery dataset where the output tables will be created[cite: 14, 16]."tables": A nested dictionary specifying the three destination tables created by PySpark:  "transformed_table": Destination table for the row-level transformed dataset.  "route_insights_table": Destination table for route-level aggregations.  "origin_insights_table": Destination table for booking origin aggregations.  