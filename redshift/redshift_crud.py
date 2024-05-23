import psycopg2
import pandas as pd
import crud_logger


log = crud_logger.init_logger()
# JDBC url
# jdbc:redshift://my-serverless-workgroup.431674898822.ap-southeast-1.redshift-serverless.amazonaws.com:5439/mydatabase

RS_HOST = "my-serverless-workgroup.431674898822.ap-southeast-1.redshift-serverless.amazonaws.com"
RS_PORT = "5439"
RS_DATABASE = "mydatabase"
RS_USER = "devuser"
RS_PASSWORD = "Devdbpass7"

con = psycopg2.connect(host = RS_HOST,
                       port = RS_PORT,
                       dbname = RS_DATABASE,
                       user = RS_USER,
                       password = RS_PASSWORD)

cursor = con.cursor()

def create_table():
    try:   
        cursor.execute('create table public.emp(id int, name varchar(50))')
    except Exception as e:
        log.exception(e)
    else:
        con.commit()
        log.info('table created')


def insert_data():
   
    # Define the COPY command
    copy_command = """
       COPY mydatabase.public.emp FROM 's3://sushi-bucket/emp.csv' IAM_ROLE 'arn:aws:iam::431674898822:role/redshifS3Access' FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'ap-southeast-1'
    """

    # Execute the COPY command
    try:
        cursor.execute(copy_command)
    except Exception as e:
        log.exception(e)
    else:
        con.commit()
        log.info('data inserted')

def select_data():
    try :
        cursor.execute('select * from emp')
        rows = cursor.fetchall()
    except Exception as e:
        log.exception(e)
    # Iterating through the rows and printing the results
    else:
        log.info('rows fetched')
        for row in rows:
            print(row)

if __name__ == "__main__":
    # create_table()
    # insert_data()
    select_data()

