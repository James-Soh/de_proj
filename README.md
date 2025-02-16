## Overview


This project utilizes open source tools to build an ELT pipeline to orchestrate aviation data from [AviationStack](https://aviationstack.com/) to a locally hosted [MinIO](https://min.io/) S3 compatible storage service to PostgreSQL databases which serves as a Data Warehouse.

<!--
The repository directory structure is as follows:
```
├── assets/                        <- Includes assets for the repo.
│   └── (Contains images, architecture and quicksight dashboard)
│
├── data/                          <- Contains data used and processed by the project.
│   ├── raw/                      <- Raw data files (not included here due to large files size).
│   ├── cleansed/                 <- Cleansed data files.
│   └── analytics/                <- Materialized view for analytics and reporting.
│
├── docs/                          <- Documentation for the project.
│   └── solution methodology.pdf   <- Detailed project documentation.
│
├── scripts/                                       <- Python scripts for the ETL pipeline.
│   ├── etl_pipeline_csv_to_parquet.py             <- csv to parquet pipeline glue script.
│   ├── lambda_function.py                         <- Lambda function code.
│   └── etl_pipeline_materialised_view.py          <- materialised view pipeline glue script
│
├── README.md                      <- The top-level README for developers using this project.

```
-->


## Tools 

To build this project, the following tools were used:

- Python
- Minio
- PostgresSQL


<!--
## Architecture

Following is the architecture of the project.

<p align='center'>
  <img src='https://github.com/waqarg2001/Youtube-Data-Pipeline-AWS/blob/main/assets/AWS_Python_ETL_Project_Architecture.png' height=385 width=650>
</p>  

## Dashboard

Access simplified dashboard from <a href='https://github.com/waqarg2001/Youtube-Data-Pipeline-AWS/blob/main/assets/dashboard.pdf'>here</a>.


## Screenshots

Following are project execution screenshots from AWS portal.

<img src="https://github.com/waqarg2001/Youtube-Data-Pipeline-AWS/blob/main/assets/ss1.png" width=900 height=400>
<br>
<img src="https://github.com/waqarg2001/Youtube-Data-Pipeline-AWS/blob/main/assets/ss2.png" width=900 height=400>

## Support

If you have any doubts, queries, or suggestions then, please connect with me on any of the following platforms:

[![Linkedin Badge][linkedinbadge]][linkedin] 
[![Gmail Badge][gmailbadge]][gmail]
-->