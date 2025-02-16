
## Overview


This project utilizes 

AWS Cloud Services to build an efficient ETL pipeline for processing YouTube video statistics data. The data, available [here](https://kaggle.com/datasnaek/youtube-new), is downloaded from Kaggle and uploaded to an S3 bucket. AWS Glue catalogs the data, enabling seamless querying using Amazon Athena. The pipeline processes both JSON and CSV data, converting them into Parquet format. JSON data is transformed using AWS Lambda functions with AWS Data Wrangler layers, while CSV data is processed through visual ETL jobs in AWS Glue.

Data is first stored in a Minio bucket (S3 bucket compatible), 

then cleaned and organized in a cleansed bucket, and finally joined and stored in an analytics or materialized bucket. Automated ETL jobs run daily using AWS Glue workflows, ensuring up-to-date data processing. A simple QuickSight dashboard visualizes the cleansed data, providing valuable insights into YouTube video performance across different regions. This setup ensures a scalable and efficient data processing workflow, facilitating detailed analysis and reporting.



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



## Tools 

To build this project, the following tools were used:

- Python
- Minio
- PostgresSQL