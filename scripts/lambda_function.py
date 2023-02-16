import json
import boto3

def lambda_handler(event, context):
    # S3 Client
    s3 = boto3.client('s3')

    #Source info
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']

    #Destination info
    destination_bucket = event['Records'][0]['s3']['bucket']['name']
    destination_key = 'raw/' + source_key.split('/')[1]
 
    # Glue Client
    glue = boto3.client('glue')
    job_name = 'spark-processing-staging'

    # Start the Glue job
    response = glue.start_job_run(JobName=job_name)
    
    return {
        'statusCode': 200,
        'body': 'File copied successfully'
    }