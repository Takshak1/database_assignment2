"""
Clear all data from MySQL and MongoDB
"""
import os
from dotenv import load_dotenv
import mysql.connector
from pymongo import MongoClient

load_dotenv()

try:
    mysql_conn = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', 'devil'),
        database=os.getenv('MYSQL_DATABASE', 'streaming_db')
    )
    cursor = mysql_conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS logs")
    mysql_conn.commit()
    cursor.close()
    mysql_conn.close()
    print("MySQL table 'logs' dropped")
except Exception as e:
    print(f"MySQL error: {e}")

try:
    mongo_client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=3000)
    db = mongo_client['streaming_db']
    result = db['logs'].delete_many({})
    print(f"MongoDB collection 'logs' cleared ({result.deleted_count} documents deleted)")
    mongo_client.close()
except Exception as e:
    print(f"MongoDB error: {e}")

print("\nDatabases cleared successfully")
