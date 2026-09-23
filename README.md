# Employee Data Analysis Pipeline (Airflow & Ephemeral Dataproc)

## Overview
This project demonstrates an automated data processing pipeline on Google Cloud Platform (GCP). It uses Cloud Composer (Apache Airflow) to orchestrate an ephemeral Dataproc cluster that runs a PySpark job to process and join employee and department data, and then tears down the cluster to optimize costs.

## Pipeline Flow
The core workflow orchestrated by the Airflow DAG (`employee_data_analyis_with_spark`) follows these three exact steps:
1. **Create Dataproc Cluster:** Provisions a temporary Dataproc cluster (`dataproc-spark-airflow-demo`) with 1 master and 2 worker nodes.
2. **Run PySpark Job:** Submits a batch job (`emp_batch_job.py`) to the cluster to filter and join data.
3. **Delete Dataproc Cluster:** Tears down the cluster upon completion (using the `all_done` trigger rule, ensuring deletion even if the job fails).

<p align="center"><img width="980" height="760.8" alt="draw_io_flowchart" src="https://github.com/user-attachments/assets/90ff2c7f-66ac-416f-b47e-7f0537b3cb81" /></p>


## Tech Stack
* **Orchestration:** Apache Airflow (GCP Cloud Composer)
* **Processing:** PySpark, GCP Dataproc
* **Storage:** Google Cloud Storage (GCS)

## Google Cloud Storage (GCS) Folder Structure
The pipeline relies on a specific GCS bucket structure to organize the data and scripts:

```text
gs://airflow-test-projects-gds-dev/
└── airflow-project-1/
    ├── data/
    │   ├── employee.csv        # Source data: Employee details
    │   └── department.csv      # Source data: Department details
    ├── output/                 # Destination for joined PySpark output
    └── spark-job/
        └── emp_batch_job.py    # PySpark transformation script
```

*(Note: Cloud Composer also provisions its own separate bucket for storing DAGs and Airflow configuration files).*

## PySpark Transformation
The `emp_batch_job.py` script performs the following operations:
1. Reads `employee.csv` and `department.csv` from the GCS `data/` folder.
2. Filters the employee records to keep only those with a salary greater than 50,000.
3. Performs an inner join between the filtered employee data and department data on `dept_id`.
4. Writes the processed, joined dataset to the `output/` folder in CSV format.

## Step-by-Step Execution Guide

1. **Setup Airflow Environment:** Initialize your Cloud Composer (Airflow) environment. This will automatically generate a linked GCS bucket for your Airflow files (DAGs, plugins, etc.).
2. **Setup Data Bucket:** Create a separate bucket named `airflow-test-projects-gds-dev`. Inside it, create the directory structure outlined above (`data/`, `output/`, `spark-job/`). Upload `employee.csv` and `department.csv` to the `data/` folder, and upload `emp_batch_job.py` to the `spark-job/` folder.
3. **Deploy the DAG:** Upload the `airflow_spark_job.py` file to the `dags/` folder in your Cloud Composer environment's GCS bucket.
4. **Monitor the Web UI:** Once uploaded, Airflow will parse the file. You will soon see the `employee_data_analyis_with_spark` DAG appear on the Airflow Web UI.
  <p align="center"> <img width="959.5" height="450.5" alt="screenshot_airflow_graph" src="https://github.com/user-attachments/assets/e8c1c7dd-37fa-421e-b47a-c1accff4108e" /></p>
5. **Track Progress:** Trigger the DAG and navigate to the **Graph** section in the Airflow UI to monitor the progress of the workflow visually in real-time.
  <p align="center"> <img width="953" height="430" alt="screenshot_airflow_success" src="https://github.com/user-attachments/assets/71b7306c-77b2-4dbc-bc5e-647c28e5b0af" /></p>
6. **Execution Lifecycle:** 
   * As soon as Task 1 (`create_dataproc_cluster`) succeeds, you will see the cluster spin up in your GCP Dataproc console.
   * Task 2 (`submit_pyspark_job_on_dataproc`) will run the data transformation.
   * On completion (or failure) of the Spark job, Task 3 (`delete_dataproc_cluster`) executes automatically, safely tearing down the cluster resources.

<p align="center"> <img width="959.5" height="423" alt="screenshot_gcs_output" src="https://github.com/user-attachments/assets/58094e16-dbfe-4f01-9e7a-acd148b4baae" /></p>
