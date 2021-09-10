import boto3
import os
from urllib.parse import unquote_plus
import csv
import json
import logging

table_name = os.getenv('TABLE_NAME')

def create_clients(services):
    clients = {}
    for service in services:
        clients[service] = boto3.client(service)
    return clients


def download_csv(s3_client, bucket, key, download_path):
    s3_client.download_file(bucket, key, download_path)


def process_csv(csv_name, csv_path):
    csv_data = {}
    csv_data['csv_id'] = csv_name
    csv_data['rows'] = []
    with open(csv_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            csv_data['rows'].append(row)
    return csv_data

# This structure can be refined, but works better when CSV has a defined structure, then the table can be made a lot more useful
def put_csv_data(dynamodb_client, csv_data):
    response = dynamodb_client.put_item(
        TableName=table_name,
        Item={
            'csvId': {
                'S': csv_data['csv_id']
            },
            'contents':{
                'S': json.dumps(csv_data['rows'])
            }
        }
    )


def handler(event, context):
    logging.info('Creating boto3 clients')
    clients = create_clients(['s3', 'dynamodb'])

    logging.info('Processing S3 objects')
    for record in event['Records']:
        bucket = unquote_plus(record['s3']['bucket']['name'])
        key = record['s3']['object']['key']
        subbed_key = key.replace('/', '-')
        download_path = f'/tmp/{subbed_key}'

        logging.info(f'Downloading {key} from {bucket}')
        download_csv(clients['s3'], bucket, key, download_path)
        
        logging.info('Extracting data from CSV')
        csv_data = process_csv(subbed_key, download_path)

        logging.info(f'Inserting CSV data into {table_name}')
        put_csv_data(clients['dynamodb'], csv_data)