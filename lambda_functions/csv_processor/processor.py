import boto3
import os
from urllib.parse import unquote_plus
import csv
import json
import logging

table_name = os.getenv("TABLE_NAME")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def create_clients(services):
    clients = {}
    for service in services:
        clients[service] = boto3.client(service)
    return clients


def download_csv(s3_client, bucket, key, download_path):
    s3_client.download_file(bucket, key, download_path)


def extract_csv_data(csv_path):
    csv_data = {}
    csv_data["rows"] = []
    with open(csv_path, "r") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            if "customer_id" in row:
                csv_data["rows"].append(row)
            else:
                logger.error({"item": row, "message": "customer_id not present"})
    return csv_data


def transform_csv_data(csv_data):
    dynamodb_items = []
    if "rows" in csv_data:
        for i in csv_data["rows"]:
            item = {}
            for key, value in i.items():
                item[key] = {"S": value}
            dynamodb_items.append(item)

    return dynamodb_items


# This structure can be refined, but works better when CSV has a defined structure, then the table can be made a lot more useful
def load_dynamodb_items(dynamodb_client, dynamodb_items):
    x = 0
    for item in dynamodb_items:
        try:
            dynamodb_client.put_item(TableName=table_name, Item=item)
            x += 1
        except Exception as e:
            logger.error(
                {
                    "item": item,
                    "message": "Error writing item to dynamodb",
                    "exception": e,
                }
            )
    return f"{x} of {len(dynamodb_items)} items written to dynamodb"


def handler(event, context):
    logger.info("Creating boto3 clients")
    clients = create_clients(["s3", "dynamodb"])

    logger.info("Processing S3 objects")
    for record in event["Records"]:
        bucket = unquote_plus(record["s3"]["bucket"]["name"])
        key = record["s3"]["object"]["key"]
        subbed_key = key.replace("/", "-")
        download_path = f"/tmp/{subbed_key}"

        logger.info(f"Downloading {key} from {bucket}")
        download_csv(clients["s3"], bucket, key, download_path)

        logger.info(f"Extracting data from {key}")
        csv_data = extract_csv_data(download_path)

        logger.info(f"Transforming data from {key}")
        dynamodb_items = transform_csv_data(csv_data)

        logger.info(f"Loading data from {key} into {table_name}")
        load_dynamodb_items(clients["dynamodb"], dynamodb_items)
