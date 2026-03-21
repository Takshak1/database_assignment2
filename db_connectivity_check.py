import mysql.connector
from pymongo import MongoClient
import sys

import os
from dotenv import load_dotenv
import mysql.connector
from pymongo import MongoClient
import sys

load_dotenv()

try:
    c = mysql.connector.connect(
        host=os.getenv('MYSQL_HOST', 'localhost'),
        user=os.getenv('MYSQL_USER', 'root'),
        password=os.getenv('MYSQL_PASSWORD', 'devil'),
        database=os.getenv('MYSQL_DATABASE', 'streaming_db'),
        connection_timeout=5
    )
    print('MYSQL_OK')
    c.close()
except Exception as e:
    print('MYSQL_ERR', e)

try:
    client = MongoClient(f"mongodb://{os.getenv('MONGO_HOST','localhost')}:{os.getenv('MONGO_PORT',27017)}/", serverSelectionTimeoutMS=3000)
    client.server_info()
    print('MONGO_OK')
    client.close()
except Exception as e:
    print('MONGO_ERR', e)

sys.exit(0)
