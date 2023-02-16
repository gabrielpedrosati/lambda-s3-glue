# Processing Data With Glue Job From a Lambda Trigger

## General Information
I have an S3 Bucket called `staging/` that receives the arriving data, it could be from an API, Databases, Social Media... So, when the data arrives, this action will trigger a Lambda function that will start a Glue Job to process this new data and save it in the `parquet` format into an S3 Bucket called `raw/`. The `raw/` bucket stores data with some basic cleansing and transformations.

Process Step:
- Rename values from the columns: type_of_meal_plan and booking_status (improve reading)
- Rename column names (improve reading)
- Convert from csv to parquet (smaller size) 
- Partition parquet file by year (for better analysis)
- Create a bucket with the date of the processing step - `raw/hotel-reservations/data/dataload={}/` (track when the data was processed)

## Architecture
!["Architecture"](/imgs/architecture.png)
## Resources
- AWS Lambda
- AWS S3
- AWS Glue
## Snapshots
Lambda Function
!["Lambda Function"](/imgs/lambda_function.png)

Lambda Trigger Configuration
!["Trigger Configuration"](/imgs/trigger_config.png)

S3 Staging
!["Staging"](/imgs/staging.png)

Glue Job
!["Glue Job"](/imgs/gluejob.png)

S3 Raw
!["Raw"](/imgs/raw.png)

## Contact
Feel free to contact me - [Gabriel Pedrosa](https://www.linkedin.com/in/gabrielpedrosati/).
