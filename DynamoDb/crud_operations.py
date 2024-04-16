import boto3
import os
from dotenv import load_dotenv
load_dotenv()

dynamo_client = boto3.client(service_name = 'dynamodb',
                      region_name = 'ap-southeast-1',
                      aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
                      aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
                      )

# function to create a table
def create_table():
    table_name = 'EmployeeTable'

    key_schema =[{
        'AttributeName' : 'empid',
        'KeyType' : 'HASH'
    }]

    attribute_definitions = [{
        'AttributeName' : 'empid',
        'AttributeType' : 'S'
    }]

    provisioned_throughput = {
         'ReadCapacityUnits': 5,
         'WriteCapacityUnits': 5

    }

    response = dynamo_client.create_table(
        TableName = table_name,
        KeySchema = key_schema,
        AttributeDefinitions = attribute_definitions,
        ProvisionedThroughput = provisioned_throughput

    )

    print('Table created')

def list_all_tables():
    response = dynamo_client.list_tables()['TableNames']
    for res in response:
        print(res)

def put_item():
    table_name = 'EmployeeTable'
    dynamo_client.put_item(TableName = table_name,                   
        Item = {
            'empid':{'S':'e103'},
            'name' : {'S':'Ryan'},
            'salary':{'N': '50000'},
        })

def select_all_items():
    table_name = 'EmployeeTable'
    response = dynamo_client.scan(TableName = table_name)
    for row in response['Items']:
        print(row)

def select_specific_record():
    table_name = 'EmployeeTable'
    response = dynamo_client.query(
        TableName=table_name,
        KeyConditionExpression='empid = :id',
        ExpressionAttributeValues={
            ':id': {'S': 'e101'}  # Replace '1' with the specific empid you want to query
        }
    )
    print(response['Items'])

def update():
    table_name = 'EmployeeTable'
    update_expression = 'SET salary = :new_salary'
    expression_values = {
        ':new_salary' : {'N': str(90000)}
    }

    dynamo_client.update_item(TableName = table_name,
                              Key = {'empid':{'S': 'e101'}},
                              UpdateExpression = update_expression,
                              ExpressionAttributeValues = expression_values
                              )
    print('Item updated successfully')

def delete_item():
    table_name = 'EmployeeTable'
    dynamo_client.delete_item(TableName=table_name,
                              Key = {'empid':{'S':'e103'}}
                              )
    print('Record deleted successfully!')


# list_all_tables()
# put_item()
# select_all_items()
# select_specific_record()
# update()
# delete_item()