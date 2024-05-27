import boto3
import logger
import os 
from dotenv import load_dotenv
load_dotenv()
# Initialize the Redshift client
redshift = boto3.client('redshift-serverless')

log = logger.init_logger()

def create_namespace(namespace_name, db_name):
    try:
        response = redshift.create_namespace(   adminUserPassword=os.environ.get('RS_PASSWORD'),
                                                adminUsername= os.environ.get('RS_USER'),
                                                dbName=db_name,
                                                namespaceName=namespace_name,
                                                manageAdminPassword=False,
                                                defaultIamRoleArn='arn:aws:iam::431674898822:role/redshifS3Access',
                                                iamRoles=[
                                                            'arn:aws:iam::431674898822:role/redshifS3Access',
                                                        ]

                                            )
        log.info(f"Namespace {namespace_name} created successfully.")
        return response
    except Exception as e:
        log.exception(f"Error creating namespace: {e}")
        return None

def create_workgroup(workgroup_name, namespace_name):
    try:
        response = redshift.create_workgroup(
            workgroupName=workgroup_name,
            namespaceName=namespace_name
        )
        log.info(f"Workgroup {workgroup_name} created successfully.")
        return response
    except Exception as e:
        log.exception(f"Error creating workgroup: {e}")
        return None

def main():
    # Define the namespace and workgroup names
    namespace_name = 'my-serverless-namespace'
    db_name = 'mydatabase'
    workgroup_name = 'my-serverless-workgroup'
    
    # Create a namespace
    namespace_response = create_namespace(namespace_name, db_name)
    if namespace_response:
        # Create a workgroup
        workgroup_response = create_workgroup(workgroup_name, namespace_name)
        if workgroup_response:
            log.info("Redshift Serverless setup completed successfully.")
        else:
            log.error("Failed to create workgroup.")
    else:
        log.error("Failed to create namespace.")

if __name__ == "__main__":
    main()
