import boto3
import pymysql
import csv
import os

# Database connection parameters
DB_HOST = 'localhost'
DB_PORT = 8005  # Mapped port from Docker container
DB_USER = 'root'
DB_PASSWORD = 'utec'
DB_NAME = 'bd_api_employees'  # We'll need to specify which database/table to read from
TABLE_NAME = 'employees'

# File and S3 configuration
ficheroUpload = "data.csv"
nombreBucket = "gcr-output-01"

def connect_to_mysql():
    """Connect to MySQL database"""
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Successfully connected to MySQL database")
        return connection
    except Exception as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def get_all_tables(connection):
    """Get list of all databases and tables"""
    try:
        with connection.cursor() as cursor:
            # Get all databases
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            print("Available databases:")
            for db in databases:
                print(f"  - {db['Database']}")
            
            # For each database (except system ones), show tables
            for db in databases:
                db_name = db['Database']
                if db_name not in ['information_schema', 'performance_schema', 'mysql', 'sys']:
                    cursor.execute(f"USE {db_name}")
                    cursor.execute("SHOW TABLES")
                    tables = cursor.fetchall()
                    if tables:
                        print(f"\nTables in database '{db_name}':")
                        for table in tables:
                            table_name = list(table.values())[0]
                            print(f"  - {table_name}")
        
    except Exception as e:
        print(f"Error getting database information: {e}")

def read_data_from_mysql(connection, database_name=DB_NAME, table_name=TABLE_NAME):
    """Read all data from MySQL database"""
    try:
        with connection.cursor() as cursor:
            # If database is specified but table is not, read from all tables in that database
            if database_name and not table_name:
                cursor.execute(f"USE {database_name}")
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                
                if not tables:
                    print(f"No tables found in database '{database_name}'")
                    return []
                
                all_data = []
                for table in tables:
                    table_name = list(table.values())[0]
                    cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                    count = cursor.fetchone()['count']
                    
                    if count > 0:
                        print(f"Reading {count} rows from table '{table_name}' in database '{database_name}'")
                        cursor.execute(f"SELECT * FROM {table_name}")
                        table_data = cursor.fetchall()
                        all_data.extend(table_data)
                
                return all_data
            
            # If both database and table are specified
            elif database_name and table_name:
                cursor.execute(f"USE {database_name}")
                cursor.execute(f"SELECT * FROM {table_name}")
                return cursor.fetchall()
            
            # If no specific database/table specified, try to find data (original logic)
            else:
                print("No specific database/table specified. Scanning for data...")
                get_all_tables(connection)
                
                # Try to find a database with data
                cursor.execute("SHOW DATABASES")
                databases = cursor.fetchall()
                
                for db in databases:
                    db_name = db['Database']
                    if db_name not in ['information_schema', 'performance_schema', 'mysql', 'sys']:
                        cursor.execute(f"USE {db_name}")
                        cursor.execute("SHOW TABLES")
                        tables = cursor.fetchall()
                        
                        for table in tables:
                            table_name = list(table.values())[0]
                            cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                            count = cursor.fetchone()['count']
                            
                            if count > 0:
                                print(f"Found data in {db_name}.{table_name} ({count} rows)")
                                cursor.execute(f"SELECT * FROM {table_name}")
                                return cursor.fetchall()
                
                print("No data found in any table")
                return []
                
    except Exception as e:
        print(f"Error reading data from MySQL: {e}")
        return []

def save_to_csv(data, filename):
    """Save data to CSV file"""
    if not data:
        print("No data to save")
        return False
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            if isinstance(data[0], dict):
                # If data is list of dictionaries
                fieldnames = data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            else:
                # If data is list of tuples/lists
                writer = csv.writer(csvfile)
                writer.writerows(data)
        
        print(f"Data successfully saved to {filename}")
        return True
    except Exception as e:
        print(f"Error saving data to CSV: {e}")
        return False

def upload_to_s3(filename, bucket_name):
    """Upload file to S3 bucket"""
    try:
        s3 = boto3.client('s3')
        response = s3.upload_file(filename, bucket_name, filename)
        print(f"File {filename} successfully uploaded to S3 bucket {bucket_name}")
        return response
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return None

def main():
    """Main function to orchestrate the data ingestion process"""
    print("Starting data ingestion process...")
    
    # Connect to MySQL
    connection = connect_to_mysql()
    if not connection:
        print("Failed to connect to MySQL. Exiting.")
        return
    
    try:
        # Read data from MySQL using the specified database
        data = read_data_from_mysql(connection, DB_NAME)
        
        if data:
            print(f"Successfully retrieved {len(data)} rows from database")
            
            # Save to CSV
            if save_to_csv(data, ficheroUpload):
                # Upload to S3
                upload_response = upload_to_s3(ficheroUpload, nombreBucket)
                if upload_response is not None:
                    print("Ingesta completada successfully")
                else:
                    print("Ingesta completed with S3 upload error")
            else:
                print("Ingesta failed: Could not save data to CSV")
        else:
            print("Ingesta failed: No data found in database")
            
    finally:
        connection.close()
        print("Database connection closed")

if __name__ == "__main__":
    main()