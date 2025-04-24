import json
import boto3
from botocore.exceptions import ClientError

# AWS Clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Configuration
BUCKET_NAME = "cloudaillc-vivek"
FILE_KEY = "Program_Catalog_Pipeline/output/allprograms.json"
TABLE_NAME = "program_data"

# Reference to DynamoDB table
table = dynamodb.Table(TABLE_NAME)

# Function to read JSON file from S3
def read_data_from_s3(bucket_name, file_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)  # Load JSON as Python list of dictionaries
    except ClientError as e:
        print(f"Error reading S3 file {file_key}: {e}")
        return None

# Function to insert data into DynamoDB
def load_data_to_dynamodb(data):
    success_count = 0
    failed_count = 0
    failed_items = []

    for record in data:
        try:
            # Ensure required fields exist
            if "programTitle" not in record or "department" not in record:
                print(f"Skipping record due to missing keys: {record}")
                failed_count += 1
                continue

            record['department'] = record['department'] if record['department'] else 'Not Provided'

            # Insert data into DynamoDB
            table.put_item(
                Item= record
                )
            success_count += 1

        except Exception as e:
            print(f"Error inserting record: {str(e)}")
            failed_count += 1
            failed_items.append(record)

    return success_count, failed_count, failed_items

# Lambda Handler Function
def lambda_handler(event, context):
    # Step 1: Read data from S3
    data = read_data_from_s3(BUCKET_NAME, FILE_KEY)
    if data is None:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Failed to fetch data from S3."})
        }

    # Step 2: Load data into DynamoDB
    success, failed, failed_records = load_data_to_dynamodb(data)

    # Step 3: Return results
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Successfully processed {success} records. Failed {failed} records.",
            "failed_records": failed_records
        })
    }
