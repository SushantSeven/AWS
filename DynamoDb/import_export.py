import boto3
import csv
import os
import boto3.resources
from dotenv import load_dotenv
load_dotenv()

dynamo_client = boto3.resource(service_name = 'dynamodb',
                             region_name = 'us-east-1',
                             aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
                             aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
                             )
# function to import file into dynamo db
def import_file():
    table = dynamo_client.Table('iris')
    rows = csv.DictReader(open('customers-100.csv'))
    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item=row)
    print('Data imported successfully')

# function to export file from dynamo db
def export_data():
    table = dynamo_client.Table('iris')
    response = table.scan()
    items = response['Items']
    with open('D:\CFP_DATA_ENGINEERING_LYTX\AWS_repo\AWS\DynamoDb\exported-customers.csv', 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=items[0].keys())
        writer.writeheader()
        for item in items:
            writer.writerow({key: value for key, value in item.items()})
    print('data exported')


if __name__ == '__main__':
    # import_file()
    export_data()