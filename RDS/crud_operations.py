
import pymssql
import os
import logger
from dotenv import load_dotenv
load_dotenv('.env')

log = logger.init_logger()

connection={
       'host': 'demo-ms.ckwvgvz0hkmb.ap-southeast-1.rds.amazonaws.com',
    'username': os.getenv('RDSUSERNAME'),
    'password': os.getenv('PASSWORD'),
    'db': 'pydb' 
 }

con=pymssql.connect(connection['host'],connection['username'],connection['password'],connection['db'])
cursor = con.cursor()

# funtion to perform create table
def create_table():
    query = "create table employee (empid nvarchar(20), age int, salary int)"
    cursor.execute(query)
    log.info('Table created')
    con.commit()
    cursor.close()
    con.close()

# funtion to perform insert operation
def insert_data():
     try:
        emp_id = int(input("Employeeid : "))
        age = int(input("age : "))
        salary = int(input("salary : "))
     except ValueError:
        log.exception('Value error')
     else:
        try:    
            cursor.execute(f'insert into employee values({emp_id}, {age}, {salary});')
        except Exception as e:
            log.exception(e)
        else:
            con.commit()
            log.info('Data inserted')
            cursor.close()
            con.close()

def view_data():
    cursor.execute('select * from employee')
    for row in cursor:
        print(row)
    log.info('data viewed')
    cursor.close()
    con.close()

# funtion to perform update operation
def update_data():
    try:
        emp_id = int(input("Employeeid : "))
    except Exception as e:
        log.exception(e)
    else:
        try:
            cursor.execute(f"update employee set salary=800000 where empid = {emp_id}")
        except Exception as e:
            log.exception(e)
        else:
            cursor.close()
            con.close()
            con.commit()
            log.info('Data updated')


# funtion to perform delete operation
def delete_data():
    try:
        emp_id = int(input("Employeeid : "))
    except Exception as e:
        log.exception(e)
    else:
        try:
            cursor.execute(f"delete from employee where empid = {emp_id}")
        except Exception as e:
            log.exception(e)
        else:
            cursor.close()
            con.close()
            con.commit()
            log.info('Data deleted')
            
if __name__ == "__main__":
    # create_table()
    # insert_data()
    # update_data()
    # delete_data()
    view_data()

