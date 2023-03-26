# <p align=center>Apache Airflow with AWS S3</p>

The project in question was created to perform a complete data pipeline. Apache Airflow acted as the orchestrator of this pipeline, with two DAGs. The first DAG runs daily to ingest data directly from the API into the bronze layer in the S3 bucket. The second DAG is triggered by an S3 sensor as soon as a new file arrives in the bucket. This DAG is responsible for processing the data, passing it from the silver layer to the gold layer, from which only consolidated data is extracted and saved in a table in PostgresSQL.

At the end, a report is displayed in Power BI containing the data extracted from the database.

*Note: The extracted data is fictional and provided by the [Random User API.](https://randomuser.me)*

# <p align=center>Data Pipeline Architecture</p>
<p align="center">
  <img src="https://github.com/ThiagoFerreiraWD/Pipeline_ETL_Airflow_S3Bucket_Postgres/blob/main/architecture.png">
</p>

<p align="center">
  <img src="https://github.com/ThiagoFerreiraWD/Pipeline_ETL_Airflow_S3Bucket_Postgres/blob/main/dag_ingest_api.png">
</p>

<p align="center">
  <img src="https://github.com/ThiagoFerreiraWD/Pipeline_ETL_Airflow_S3Bucket_Postgres/blob/main/dag_sensor_s3bucket.png">
</p>

***

## Tools and languages used:
<div>
<img width=30 src="https://avatars.githubusercontent.com/u/33643075?s=200&v=4" />
<img width=40 src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" />
<img width=30 src="https://user-images.githubusercontent.com/15157491/75435753-6929fc80-594b-11ea-9e19-f78223916862.png" />
</div>

## Description of the files:

1. **dags/dag_ingest_api.py:** Python code for the main DAG triggered daily and responsible for ingesting raw data into the S3 bucket;
1. **dags/dag_sensor_s3bucket.py:** Second DAG, responsible for processing and consolidating data up to the final table in PostgresSQL;
1. **dags/scripts_dag_ingestion_api.py:** Script containing Python functions used in the DAG "dag_ingest_api.py";
1. **dags/scripts_dag_transform_data.py:** Script containing Python functions used in the DAG "dag_sensor_s3bucket.py";
1. **sql/criar_tabela_dw_users.sql:** SQL code responsible for creating a table in PostgreSQL;
1. **architecture.png:** Designed architecture;
1. **customer_report.pbix:** Final Power BI report;
1. **dag_ingest_api.png:** Image of the successful execution of DAG "dag_ingest_api.py";
1. **dag_sensor_s3bucket.png:** Image of the successful execution of DAG "dag_sensor_s3bucket.py";
1. **docker-compose.yaml:** Docker Compose file with the configurations for Apache Airflow and PostgreSQL; and
1. **report_customers.jpg:** Example image of the Power BI report. Note that the data is fictitious, provided by the Random User API.

## Contacts:
<div>   
  <a href="https://www.linkedin.com/in/tferreirasilva/">
    <img width=40 src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/linkedin/linkedin-original.svg" />
  </a> 
  <a href = "mailto:thiago.ferreirawd@gmail.com">
      <img width=40 src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/google/google-original.svg" />
  </a>  
  <a href = "https://github.com/ThiagoFerreiraWD">
    <img width=40 src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/github/github-original.svg" />
  </a>     
</div>
