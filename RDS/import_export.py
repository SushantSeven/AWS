import pymssql
import csv
import os

from dotenv import load_dotenv
load_dotenv('.env')

connection = { 'host' : 'demo-ms.ckwvgvz0hkmb.ap-southeast-1.rds.amazonaws.com',
              'username' : os.environ.get('RDSUSERNAME'),
              'password' : os.environ.get('PASSWORD'),
              'db' : 'pydb'

}

con = pymssql.connect(connection['host'], connection['username'], connection['password'], connection['db'])
cursor = con.cursor()


# creating a table
def create_table():
    query = "create table customer (Customer_Id nvarchar(50),First_Name nvarchar(20),Last_Name nvarchar(20),Company nvarchar(60),City nvarchar(50),Country nvarchar(50),Phone_1 nvarchar(50),Phone_2 nvarchar(50),Email nvarchar(50),Subscription_Date nvarchar(50),Website nvarchar(50))"
    cursor.execute(query)
    con.commit()
    cursor.close()
    con.close()

# importing csv file into rds
def import_csv(): 
    # reading a csv file
    csv_file = 'D:\CFP_DATA_ENGINEERING_LYTX\AWS\RDS\customers-100.csv'
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cursor.execute('insert into customer(Customer_Id,First_Name,Last_Name,Company,City ,Country ,Phone_1 ,Phone_2 ,Email ,Subscription_Date ,Website) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', 
                           (row['Customer Id'], row['First Name'], row['Last Name'], row['Company'], row['City'], row['Country'], row['Phone 1'], row['Phone 2'], row['Email'], row['Subscription Date'], row['Website']))
    con.commit()
    cursor.close()
    con.close()

# function to view data
def view_data():
    cursor.execute('select * from customer')
    for row in cursor:
        print(row)
    cursor.close()
    con.close()

def export():
    cursor.execute('select * from customer')
    csv_file = 'D:\CFP_DATA_ENGINEERING_LYTX\AWS\RDS\exported_file.csv'

    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)

        # Write the header row
        writer.writerow([i[0] for i in cursor.description])
        
        for row in cursor:
            writer.writerow(row)


if __name__ == "__main__":
    # create_table()
    # import_csv()
    export()
    # view_data()