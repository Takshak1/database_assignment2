import os
import mysql.connector
from pymongo import MongoClient
from datetime import datetime
import json
from dotenv import load_dotenv

load_dotenv()

MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', 'devil'),
    'database': os.getenv('MYSQL_DATABASE', 'streaming_db')
}

MONGO_CONFIG = {
    'host': os.getenv('MONGO_HOST', 'localhost'),
    'port': int(os.getenv('MONGO_PORT', 27017)),
    'database': os.getenv('MONGO_DATABASE', 'streaming_db'),
    'collection': os.getenv('MONGO_COLLECTION', 'logs')
}


class StorageManager:
    
    def __init__(self):
        self.mysql_conn = None
        self.mysql_cursor = None
        self.mongo_client = None
        self.mongo_collection = None
        self.sql_schema_created = False
        self.metadata = {}  
        
    def connect(self):
        try:
            self.mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
            self.mysql_cursor = self.mysql_conn.cursor()
            print("MySQL connected")
        except Exception as e:
            print(f"MySQL connection failed: {e}")
            return False
        
        try:
            uri = f"mongodb://{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}/"
            self.mongo_client = MongoClient(uri, serverSelectionTimeoutMS=3000)
            self.mongo_client.server_info()  
            db = self.mongo_client[MONGO_CONFIG['database']]
            self.mongo_collection = db[MONGO_CONFIG['collection']]
            print("MongoDB connected")
        except Exception as e:
            print(f"MongoDB connection failed: {e}")
            return False
        
        return True
    
    def initialize_schema(self, metadata):
        self.metadata = metadata
        if metadata:
            all_sql_fields = [f for f, d in metadata.items() if d == 'sql']
            if all_sql_fields:
                self.create_sql_schema(all_sql_fields)
    
    def create_sql_schema(self, sql_fields):
        if self.sql_schema_created:
            return
        
        try:
            self.mysql_cursor.execute("DROP TABLE IF EXISTS logs")
            self.mysql_conn.commit()
        except Exception as e:
            print(f"Could not drop existing table: {e}")
        
        columns = ["id BIGINT AUTO_INCREMENT PRIMARY KEY"]
        
        columns.append("username VARCHAR(255) NOT NULL")
        
        for field in sql_fields:
            if field in ['username', 'timestamp', 't_stamp', 'sys_ingested_at']:
                continue
            columns.append(f"{field} TEXT")
        
        columns.append("t_stamp VARCHAR(50)")  
        columns.append("sys_ingested_at DATETIME NOT NULL")  
        
        create_query = f"""
        CREATE TABLE IF NOT EXISTS logs (
            {', '.join(columns)},
            INDEX idx_username (username),
            INDEX idx_timestamps (t_stamp, sys_ingested_at)
        )
        """
        
        try:
            self.mysql_cursor.execute(create_query)
            self.mysql_conn.commit()
            self.sql_schema_created = True
            print(f"SQL schema created with {len(columns)} columns")
        except Exception as e:
            print(f"SQL schema creation failed: {e}")
    
    def store_record(self, record, decisions):

        t_stamp = record.get('timestamp', datetime.now().isoformat())
        sys_ingested_at = datetime.now()
        
        username = record.get('username', 'unknown')
        
        sql_data = {'username': username}
        mongo_data = {'username': username}
        
        for field, value in record.items():
            if field == 'timestamp':
                continue
            
            decision = decisions.get(field, 'mongo')
            
            if decision == 'sql':
                if not isinstance(value, (dict, list)):
                    sql_data[field] = value
                else:
                    mongo_data[field] = value
            else:
                mongo_data[field] = value
        
        sql_data['t_stamp'] = t_stamp
        sql_data['sys_ingested_at'] = sys_ingested_at.strftime('%Y-%m-%d %H:%M:%S.%f')
        mongo_data['t_stamp'] = t_stamp
        mongo_data['sys_ingested_at'] = sys_ingested_at
        
        sql_id = self._insert_sql(sql_data)        
        mongo_id = self._insert_mongo(mongo_data)
        
        return sql_id, mongo_id
    
    def _insert_sql(self, data):
        try:
            columns = list(data.keys())
            values = [data[col] for col in columns]
            
            placeholders = ', '.join(['%s'] * len(values))
            column_names = ', '.join(columns)
            
            query = f"INSERT INTO logs ({column_names}) VALUES ({placeholders})"
            self.mysql_cursor.execute(query, values)
            self.mysql_conn.commit()
            
            return self.mysql_cursor.lastrowid
        except Exception as e:
            print(f"SQL insert error: {e}")
            return None
    
    def _insert_mongo(self, data):
        try:
            result = self.mongo_collection.insert_one(data)
            return str(result.inserted_id)
        except Exception as e:
            print(f"MongoDB insert error: {e}")
            return None
    
    def get_stats(self):
        sql_count = 0
        mongo_count = 0
        
        try:
            self.mysql_cursor.execute("SELECT COUNT(*) FROM logs")
            sql_count = self.mysql_cursor.fetchone()[0]
        except:
            pass
        
        try:
            mongo_count = self.mongo_collection.count_documents({})
        except:
            pass
        
        return {'sql': sql_count, 'mongo': mongo_count}
    
    def get_linked_records_by_user(self, username, limit=10):
        sql_records = []
        mongo_records = []
        
        try:
            sql_query = """
            SELECT username, t_stamp, sys_ingested_at, id 
            FROM logs 
            WHERE username = %s 
            ORDER BY sys_ingested_at DESC 
            LIMIT %s
            """
            self.mysql_cursor.execute(sql_query, (username, limit))
            sql_results = self.mysql_cursor.fetchall()
            
            sql_columns = [desc[0] for desc in self.mysql_cursor.description]
            for row in sql_results:
                sql_records.append(dict(zip(sql_columns, row)))
        
        except Exception as e:
            print(f"SQL query error: {e}")
        
        try:
            mongo_results = self.mongo_collection.find(
                {"username": username},
                {"username": 1, "t_stamp": 1, "sys_ingested_at": 1, "_id": 1}
            ).sort("sys_ingested_at", -1).limit(limit)
            
            for doc in mongo_results:
                doc['_id'] = str(doc['_id'])
                mongo_records.append(doc)
        
        except Exception as e:
            print(f"MongoDB query error: {e}")
        
        return {
            'username': username,
            'sql_records': sql_records,
            'mongo_records': mongo_records,
            'total_sql': len(sql_records),
            'total_mongo': len(mongo_records)
        }
    
    def get_linked_records_by_timerange(self, start_time, end_time, limit=20):
        sql_records = []
        mongo_records = []
        
        try:
            sql_query = """
            SELECT username, t_stamp, sys_ingested_at, id
            FROM logs 
            WHERE sys_ingested_at BETWEEN %s AND %s
            ORDER BY sys_ingested_at ASC
            LIMIT %s
            """
            self.mysql_cursor.execute(sql_query, (start_time, end_time, limit))
            sql_results = self.mysql_cursor.fetchall()
            
            sql_columns = [desc[0] for desc in self.mysql_cursor.description]
            for row in sql_results:
                sql_records.append(dict(zip(sql_columns, row)))
        
        except Exception as e:
            print(f"SQL time-range query error: {e}")
        
        try:
            mongo_results = self.mongo_collection.find(
                {
                    "sys_ingested_at": {
                        "$gte": start_time,
                        "$lte": end_time
                    }
                },
                {"username": 1, "t_stamp": 1, "sys_ingested_at": 1, "_id": 1}
            ).sort("sys_ingested_at", 1).limit(limit)
            
            for doc in mongo_results:
                doc['_id'] = str(doc['_id'])
                mongo_records.append(doc)
        
        except Exception as e:
            print(f"MongoDB time-range query error: {e}")
        
        return {
            'time_range': f"{start_time} to {end_time}",
            'sql_records': sql_records,
            'mongo_records': mongo_records,
            'total_sql': len(sql_records),
            'total_mongo': len(mongo_records)
        }
    
    def demonstrate_bi_temporal_join(self):
        print("\n" + "=" * 80)
        print("                    BI-TEMPORAL JOIN DEMONSTRATION")
        print("=" * 80)
        
        try:
            sql_users = []
            mongo_users = []
            
            try:
                self.mysql_cursor.execute("SELECT DISTINCT username FROM logs LIMIT 3")
                sql_users = [row[0] for row in self.mysql_cursor.fetchall()]
            except:
                pass
            
            try:
                mongo_users = list(self.mongo_collection.distinct("username"))[:3]
            except:
                pass
            
            common_users = list(set(sql_users) & set(mongo_users))
            
            print(f"SQL Users Sample: {sql_users}")
            print(f"MongoDB Users Sample: {mongo_users}")
            print(f"Common Users (Linkable): {common_users}")
            
            if common_users:
                sample_user = common_users[0]
                print(f"\nBi-Temporal Join for User: '{sample_user}'")
                print("-" * 50)
                
                linked_data = self.get_linked_records_by_user(sample_user, limit=5)
                
                print(f"SQL Records for {sample_user}:")
                for i, record in enumerate(linked_data['sql_records'], 1):
                    print(f"  {i}. ID={record.get('id')}, t_stamp={record.get('t_stamp')}, sys_time={record.get('sys_ingested_at')}")
                
                print(f"\nMongoDB Records for {sample_user}:")
                for i, record in enumerate(linked_data['mongo_records'], 1):
                    print(f"  {i}. _id={record.get('_id')[:8]}..., t_stamp={record.get('t_stamp')}, sys_time={record.get('sys_ingested_at')}")
                
                print(f"\nLinking Summary:")
                print(f"  - Total SQL records: {linked_data['total_sql']}")
                print(f"  - Total MongoDB records: {linked_data['total_mongo']}")
                print(f"  - Linking Key: username='{sample_user}' + bi-temporal timestamps")
            
            print(f"\nTime-Range Bi-Temporal Join")
            print("-" * 40)
            from datetime import datetime, timedelta
            
            now = datetime.now()
            start_time = now - timedelta(hours=1)  
            end_time = now
            
            time_linked = self.get_linked_records_by_timerange(start_time, end_time, limit=5)
            
            print(f"Records from both backends in last hour:")
            print(f"  - SQL records: {time_linked['total_sql']}")
            print(f"  - MongoDB records: {time_linked['total_mongo']}")
            
            print(f"\nSample SQL records from time range:")
            for i, record in enumerate(time_linked['sql_records'][:3], 1):
                print(f"  {i}. User: {record.get('username')}, Time: {record.get('sys_ingested_at')}")
            
            print(f"\nSample MongoDB records from time range:")
            for i, record in enumerate(time_linked['mongo_records'][:3], 1):
                print(f"  {i}. User: {record.get('username')}, Time: {record.get('sys_ingested_at')}")
            
            print(f"\nBi-temporal join capability demonstrated successfully!")
            print(f"Key Features:")
            print(f"  - Username preservation across backends")
            print(f"  - Client timestamps (t_stamp) for historical context")
            print(f"  - Server timestamps (sys_ingested_at) for join operations")
            print(f"  - Cross-backend querying and linking")
            
        except Exception as e:
            print(f"Bi-temporal demonstration error: {e}")
    
    def close(self):
        if self.mysql_cursor:
            self.mysql_cursor.close()
        if self.mysql_conn:
            self.mysql_conn.close()
            print("MySQL closed")
        if self.mongo_client:
            self.mongo_client.close()
            print("MongoDB closed")
