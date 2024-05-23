import boto3
import json
from datetime import datetime
import calendar
import random
import time
import json
from faker import Faker
import uuid
from time import sleep
from dotenv import load_dotenv
load_dotenv()
import os

my_stream_name = 'first-lambda-streams'

kinesis_client = boto3.client(service_name = 'kinesis',
                    region_name = 'ap-southeast-1',
                    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
                    )
faker = Faker()

for i in range(1, 10):
    json_data = {
        "name":faker.name(),
        "city":faker.city(),
        "phone":faker.phone_number(),
        "id":uuid.uuid4().__str__()
    }
    print(json_data)
    sleep(0.5)

    put_response = kinesis_client.put_record(
        StreamName=my_stream_name,
        Data=json.dumps(json_data),
        PartitionKey='name')
    print(put_response)