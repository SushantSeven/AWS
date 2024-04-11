import pymssql
import os
from dotenv import load_dotenv
load_dotenv('.env')

connection={
       'host': 'demo-ms.ckwvgvz0hkmb.ap-southeast-1.rds.amazonaws.com',
    'username': os.getenv('RDSUSERNAME'),
    'password': os.getenv('PASSWORD'),
    'db': 'pydb' 
 }

con=pymssql.connect(connection['host'],connection['username'],connection['password'],connection['db'])
cursor = con.cursor()

# DDL commands

# create
def create_table():
    query = "create table student (name nvarchar(20), age int, roll int)"
    cursor.execute(query)
    con.commit()
    cursor.close()
    con.close()

# alter
def alter():
    query = "alter table student add gender nvarchar(1)"
    cursor.execute(query)
    print('Table Altered')
    con.commit()
    cursor.close()
    con.close()

# Truncate
def truncate():
    query = 'TRUNCATE TABLE student'
    cursor.execute(query)
    print('Table truncated')
    con.commit()
    cursor.close()
    con.close()

# Rename
def rename():
    query = "EXEC sp_rename student, student1"
    cursor.execute(query)
    print('Table Renamed')
    con.commit()
    cursor.close()
    con.close()

# drop
def drop():
    query = "drop table student1"
    cursor.execute(query)
    print('Table Dropped')
    con.commit()
    cursor.close()
    con.close()

# DML commands

# select
def select():
    query = "select * from student1"
    cursor.execute(query)
    if cursor:
        for row in cursor:
            print(row)
        cursor.close()
        con.close()
    else:
        print('Table is empty')

# insert
def insert():
    try:
        name = input("student name : ")
        age = int(input("age : "))
        roll = int(input("roll : "))
        gender = input("gender : ")
    except ValueError:
        # log.exception('Value error')
        print('Value error')
    else:
        try:
             query = "INSERT INTO student1 VALUES (%s, %s, %s, %s)"
             cursor.execute(query, (name, age, roll, gender))
             con.commit()
        except Exception as e:
            # log.exception(e)
            print(e)
        finally:
             cursor.close()
             con.close()


# update
def update():
    query = "UPDATE student1 SET name = 'ritu' WHERE gender = 'f'"
    cursor.execute(query)
    print('Table Updated')
    con.commit()
    cursor.close()
    con.close()

# delete
def delete():
    query = "DELETE FROM student1 WHERE name = 'ritu'"
    cursor.execute(query)
    print('Table delete')
    con.commit()
    cursor.close()
    con.close()

    


if __name__ == "__main__":
    
    # create_table()
    select()
    # alter()
    # truncate()
    # rename()
    # insert()
    # update()
    # delete()