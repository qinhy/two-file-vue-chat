# from https://github.com/qinhy/singleton-key-value-storage.git

import base64
import fnmatch
import io
import json
import os
import queue
import re
import sqlite3
import threading
import time
import unittest
import urllib
import uuid
from datetime import datetime
from PIL import Image
from typing import List, Dict
from urllib.parse import urlparse
from uuid import uuid4
from zoneinfo import ZoneInfo
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field
from fastapi import FastAPI, HTTPException

def try_if_error(func):
    try:
        func()
    except Exception as e:
        print(e)
        return e

# self checking
firestore_back = try_if_error(lambda:__import__('google.cloud.firestore')) is None
redis_back = try_if_error(lambda:__import__('redis')) is None
sqlite_back = True
aws_dynamo = try_if_error(lambda:__import__('boto3')) is None
aws_s3 = try_if_error(lambda:__import__('boto3')) is None
mongo_back = try_if_error(lambda:__import__('pymongo')) is None

class SingletonStorageController:
    def __init__(self, model):
        self.model:object = model

    def exists(self, key: str)->bool: print(f'[{self.__class__.__name__}]: not implement')

    def set(self, key: str, value: dict): print(f'[{self.__class__.__name__}]: not implement')

    def get(self, key: str)->dict: print(f'[{self.__class__.__name__}]: not implement')

    def delete(self, key: str): print(f'[{self.__class__.__name__}]: not implement')

    def keys(self, pattern: str='*')->list[str]: print(f'[{self.__class__.__name__}]: not implement')
    
    def clean(self): [self.delete(k) for k in self.keys('*')]

    def dumps(self): return json.dumps({k:self.get(k) for k in self.keys('*')})
    
    def loads(self, json_string=r'{}'): [ self.set(k,v) for k,v in json.loads(json_string).items()]

    def dump(self,path):
        data = self.dumps()
        with open(path, "w") as tf: tf.write(data)
        return data

    def load(self,path):
        with open(path, "r") as tf: self.loads(tf.read())

class PythonDictStorage:
    def __init__(self):
        self.uuid = uuid.uuid4()
        self.store = {}

class SingletonPythonDictStorage:
    _instance = None
    _meta = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SingletonPythonDictStorage, cls).__new__(cls)
            cls._instance.uuid = uuid.uuid4()
            cls._instance.store = {}
        return cls._instance
    
    def __init__(self):
        self.uuid:str = self.uuid
        self.store:dict = self.store

class SingletonPythonDictStorageController(SingletonStorageController):
    def __init__(self, model:SingletonPythonDictStorage):
        self.model:SingletonPythonDictStorage = model

    def exists(self, key: str)->bool: return key in self.model.store

    def set(self, key: str, value: dict): self.model.store[key] = value

    def get(self, key: str)->dict: return self.model.store.get(key,None)

    def delete(self, key: str):
        if key in self.model.store:     
            del self.model.store[key]

    def keys(self, pattern: str='*')->list[str]:
        return fnmatch.filter(self.model.store.keys(), pattern)

if firestore_back:
    from google.cloud import firestore
    class SingletonFirestoreStorage:
        _instance = None
        _meta = {}
        
        def __new__(cls,google_project_id:str=None,google_firestore_collection:str=None):
            same_proj = cls._meta.get('google_project_id',None)==google_project_id
            same_coll = cls._meta.get('google_firestore_collection',None)==google_firestore_collection

            if cls._instance is not None and same_proj and same_coll:
                return cls._instance
            
            if google_project_id is None or google_firestore_collection is None:
                raise ValueError('google_project_id or google_firestore_collection must not be None at first time')
            
            if cls._instance is not None and (not same_proj or not same_coll):
                cls._instance.model.close()
                print(f'warnning: instance changed to {google_project_id} , {google_firestore_collection}')

            cls._instance = super(SingletonFirestoreStorage, cls).__new__(cls)
            cls._instance.uuid = uuid.uuid4()
            cls._instance.model = firestore.Client(project=google_project_id)
            cls._instance.collection = cls._instance.model.collection(google_firestore_collection)

            cls._meta['google_project_id']=google_project_id
            cls._meta['google_firestore_collection']=google_firestore_collection

            return cls._instance
        
        def __init__(self,google_project_id:str=None,google_firestore_collection:str=None):
            self.uuid:str = self.uuid
            self.model:firestore.Client = self.model    
            self.collection:firestore.CollectionReference = self.collection
    class SingletonFirestoreStorageController(SingletonStorageController):
        def __init__(self, model: SingletonFirestoreStorage):
            self.model:SingletonFirestoreStorage = model

        def exists(self, key: str)->bool:
            doc = self.model.collection.document(key).get()
            return doc.exists
        
        def set(self, key: str, value: dict):
            self.model.collection.document(key).set(value)

        def get(self, key: str)->dict:
            doc = self.model.collection.document(key).get()
            return doc.to_dict() if doc.exists else None
        
        def delete(self, key: str):
            self.model.collection.document(key).delete()

        def keys(self, pattern: str='*')->list[str]:
            docs = self.model.collection.stream()
            keys = [doc.id for doc in docs]
            return fnmatch.filter(keys, pattern)      

if redis_back:
    import redis
    class SingletonRedisStorage:
        _instance = None
        _meta = {}

        def __new__(cls, redis_URL=None):# redis://127.0.0.1:6379
            if cls._instance is not None and cls._meta.get('redis_URL',None)==redis_URL:
                return cls._instance
            
            if redis_URL is None: raise ValueError('redis_URL must not be None at first time (redis://127.0.0.1:6379)')
            
            if cls._instance is not None and cls._meta.get('redis_URL',None)!=redis_URL:
                cls._instance.client.close()
                print(f'warnning: instance changed to url {redis_URL}')

            url:urllib.parse.ParseResult = urlparse(redis_URL)
            cls._instance = super(SingletonRedisStorage, cls).__new__(cls)                        
            cls._instance.uuid = uuid.uuid4()
            cls._instance.client = redis.Redis(host=url.hostname, port=url.port, db=0, decode_responses=True)
            cls._meta['redis_URL'] = redis_URL

            return cls._instance

        def __init__(self, redis_URL=None):#redis://127.0.0.1:6379
            self.uuid:str = self.uuid
            self.client:redis.Redis = self.client

    class SingletonRedisStorageController(SingletonStorageController):
        def __init__(self, model: SingletonRedisStorage):
            self.model:SingletonRedisStorage = model

        def exists(self, key: str)->bool:
            return self.model.client.exists(key)

        def set(self, key: str, value: dict):
            self.model.client.set(key, json.dumps(value))

        def get(self, key: str)->dict:
            res = self.model.client.get(key)
            if res: res = json.loads(res)
            return res

        def delete(self, key: str):
            self.model.client.delete(key)

        def keys(self, pattern: str='*')->list[str]:            
            try:
                res = self.model.client.keys(pattern)
            except Exception as e:
                res = []
            return res

if sqlite_back:
    class SingletonSqliteStorage:
        _instance = None
        _meta = {}

        DUMP_FILE='dump_db_file'
        LOAD_FILE='load_db_file'
               
        def __new__(cls):
            if cls._instance is None:
                cls._instance = super(SingletonSqliteStorage, cls).__new__(cls)                        
                cls._instance.uuid = uuid.uuid4()
                cls._instance.client = None
                
                cls._instance.query_queue = queue.Queue()
                cls._instance.result_dict = {}
                cls._instance.lock = threading.Lock()
                cls._instance.worker_thread = threading.Thread(target=cls._instance._process_queries)
                cls._instance.worker_thread.daemon = True
                cls._instance.should_stop = threading.Event()  # Use an event to signal the thread to stop
                cls._instance.worker_thread.start()
                cls._instance._execute_query("CREATE TABLE KeyValueStore (key TEXT PRIMARY KEY, value JSON)")
            return cls._instance

        def __init__(self):
            self.uuid:str = self.uuid
            self.client:sqlite3.Connection = self.client
            self.query_queue:queue.Queue = self.query_queue 
            self.result_dict:dict = self.result_dict 
            self.lock:threading.Lock = self.lock 
            self.worker_thread:threading.Thread = self.worker_thread 
            self.should_stop:threading.Event = self.should_stop
            
        def _process_queries(self):
            self.client = sqlite3.connect(':memory:')

            while not self.should_stop.is_set():  # Check if the thread should stop
                try:
                    # Use get with timeout to allow periodic checks for should_stop
                    query_info = self.query_queue.get(timeout=1)
                    # print('query_info',query_info)
                except queue.Empty:
                    continue  # If the queue is empty, continue to check should_stop

                query, query_id = query_info['query'], query_info['id']
                query,val = query
                query:str = query
                if SingletonSqliteStorage.DUMP_FILE == query[:len(SingletonSqliteStorage.DUMP_FILE)]:
                    try:
                        disk_conn = sqlite3.connect(query.split()[1])
                        self._clone(self.client,disk_conn)
                    except sqlite3.Error as e:
                        self._store_result(query_id, query, f"SQLite error: {e}")
                    finally:
                        disk_conn.close()
                        self.query_queue.task_done()
                        self.client.commit()   
                
                elif SingletonSqliteStorage.LOAD_FILE == query[:len(SingletonSqliteStorage.LOAD_FILE)]:     
                    try:
                        disk_conn = sqlite3.connect(query.split()[1])
                        self.client.close()
                        self.client = sqlite3.connect(':memory:')
                        self._clone(disk_conn,self.client)
                    except sqlite3.Error as e:
                        self._store_result(query_id, query, f"SQLite error: {e}")
                    finally:
                        disk_conn.close()  
                        self.query_queue.task_done()
                        self.client.commit()            
                else:
                    try:
                        cursor = self.client.cursor()
                        if val is None:
                            cursor.execute(query)
                        else:
                            cursor.execute(query, val)

                        if cursor.description is None:
                            self._store_result(query_id, query, True)
                            continue
                        columns = [description[0] for description in cursor.description]
                        result = cursor.fetchall()
                        if len(columns) > 1:
                            result = [dict(zip(columns, row)) for row in result]
                        else:
                            result = [str(row[0]) for row in result]
                        self._store_result(query_id, query, result)
                    except sqlite3.Error as e:
                        self._store_result(query_id, query, f"SQLite error: {e}")
                    finally:
                        self.query_queue.task_done()
                        self.client.commit()

        def _store_result(self, query_id, query, result):
            with self.lock:
                self.result_dict[query_id] = {'result':result,'query':query,'time':time.time()}
                # if "INSERT" or "DELETE" or "UPDATE":
                #     self.execute_query_toKafka(query)
        
        def _clone(self,a:sqlite3.Connection,b:sqlite3.Connection):
            query = "".join(line for line in a.iterdump())
            # print(query)
            b.executescript(query)
            b.commit()

        def _execute_query(self, query, val=None):
            if self.should_stop.is_set():
                raise ValueError('the DB thread is stopped!')
            query_id = str(uuid.uuid4())
            self.query_queue.put({'query': (query,val), 'id': query_id, 'time':time.time()})
            return query_id

        def _pop_result(self, query_id, timeout=1, wait=0.01):
            start_time = time.time()
            while True:
                with self.lock:
                    if query_id in self.result_dict:
                        return self.result_dict.pop(query_id)
                if time.time() - start_time > timeout:
                    return None  # or return a custom timeout message
                time.sleep(wait)  # Wait a short time before checking again to reduce CPU usage

        def _clean_result(self):
            while True:
                with self.lock:
                    self.result_dict = {}
                return True

        def _stop_thread(self):
            while not self.query_queue.empty():
                time.sleep(0.1)
            self.should_stop.set()  # Signal the thread to stop
            self.worker_thread.join()  # Wait for the thread to finish

    class SingletonSqliteStorageController(SingletonStorageController):
        def __init__(self, model: SingletonSqliteStorage):
            self.model:SingletonSqliteStorage = model

        def _execute_query_with_res(self,query):
            query_id = self.model._execute_query(query)
            result = self.model._pop_result(query_id)
            return result['result']

        def exists(self, key: str)->bool:
            query = f"SELECT EXISTS(SELECT * FROM KeyValueStore WHERE key = '{key}');"
            result = self._execute_query_with_res(query)
            return result[0]!='0'

        def set(self, key: str, value: dict):
            if self.exists(key):
                query = f"UPDATE KeyValueStore SET value = json('{json.dumps(value)}') WHERE key = '{key}'"
                result = self._execute_query_with_res(query)
            else:
                query = f"INSERT INTO KeyValueStore (key, value) VALUES ('{key}', json('{json.dumps(value)}'))"
                result = self._execute_query_with_res(query)
            return result


        def get(self, key: str)->dict:
            query = f"SELECT value FROM KeyValueStore WHERE key = '{key}'"
            result = self._execute_query_with_res(query)
            if result is None:return None
            if len(result)==0:return None
            return json.loads(result[0])
        
        def delete(self, key: str):
            query = f"DELETE FROM KeyValueStore WHERE key = '{key}'"
            return self._execute_query_with_res(query)

        def keys(self, pattern: str='*')->list[str]:
            pattern = pattern.replace('*', '%').replace('?', '_')  # Translate fnmatch pattern to SQL LIKE pattern
            query = f"SELECT key FROM KeyValueStore WHERE key LIKE '{pattern}'"
            result = self._execute_query_with_res(query)
            return result

if aws_dynamo:
    import boto3
    from botocore.exceptions import ClientError

    class SingletonDynamoDBStorage:
        _instance = None
        
        def __new__(cls,your_table_name):
            if cls._instance is None:
                cls._instance = super(SingletonDynamoDBStorage, cls).__new__(cls)
                cls._instance.uuid = uuid.uuid4()
                cls._instance.client = boto3.resource('dynamodb')
                cls._instance.table = cls._instance.client.Table(your_table_name)
            return cls._instance

        def __init__(self,your_table_name):
            self.uuid = self.uuid
            self.client = self.client
            self.table = self.table

    class SingletonDynamoDBStorageController(SingletonStorageController):
        def __init__(self, model:SingletonDynamoDBStorage):
            self.model:SingletonDynamoDBStorage = model
        
        def exists(self, key: str)->bool:
            try:
                response = self.model.table.get_item(Key={'key': key})
                return 'Item' in response
            except ClientError as e:
                print(f'Error checking existence: {e}')
                return False

        def set(self, key: str, value: dict):
            try:
                self.model.table.put_item(Item={'key': key, 'value': json.dumps(value)})
            except ClientError as e:
                print(f'Error setting value: {e}')

        def get(self, key: str)->dict:
            try:
                response = self.model.table.get_item(Key={'key': key})
                if 'Item' in response:
                    return json.loads(response['Item']['value'])
                return None
            except ClientError as e:
                print(f'Error getting value: {e}')
                return None

        def delete(self, key: str):
            try:
                self.model.table.delete_item(Key={'key': key})
            except ClientError as e:
                print(f'Error deleting value: {e}')

        def keys(self, pattern: str='*')->list[str]:
            # Convert simple wildcard patterns to regular expressions for filtering
            regex = fnmatch.translate(pattern)
            compiled_regex = re.compile(regex)

            matched_keys = []
            try:
                # Scan operation with no filters - potentially very costly
                scan_kwargs = {
                    'ProjectionExpression': "key",
                    'FilterExpression': "attribute_exists(key)"
                }
                done = False
                start_key = None

                while not done:
                    if start_key:
                        scan_kwargs['ExclusiveStartKey'] = start_key
                    response = self.model.table.scan(**scan_kwargs)
                    items = response.get('Items', [])
                    matched_keys.extend([item['key'] for item in items if compiled_regex.match(item['key'])])

                    start_key = response.get('LastEvaluatedKey', None)
                    done = start_key is None
            except ClientError as e:
                print(f'Error scanning keys: {e}')

            return matched_keys

if aws_s3:
    import boto3
    from mypy_boto3_s3 import S3Client
    from botocore.exceptions import ClientError
    class SingletonS3Storage:
        _instance = None
        _meta = {}
        
        def __new__(cls,bucket_name,
                    aws_access_key_id,aws_secret_access_key,region_name,
                    s3_storage_prefix_path = '/SingletonS3Storage'):
            meta = {                
                'bucket_name':bucket_name,
                'aws_access_key_id':aws_access_key_id,
                'aws_secret_access_key':aws_secret_access_key,
                'region_name':region_name,
            }
            def init():                
                cls._instance = super(SingletonS3Storage, cls).__new__(cls)
                cls._instance.uuid = uuid.uuid4()
                cls._instance.s3 = boto3.client('s3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    region_name=region_name
                )
                cls._instance.bucket_name = bucket_name
                cls._instance._meta = meta
            if cls._instance is None:
                init()
            elif cls._meta!=meta:                
                print(f'warnning: instance changed to new one')
                init()

            return cls._instance

        def __init__(self,bucket_name,
                    aws_access_key_id,aws_secret_access_key,region_name,
                    s3_storage_prefix_path = '/SingletonS3Storage'):
            self.uuid = self.uuid
            self.s3:S3Client = self.s3
            self.bucket_name = self.bucket_name
            self.s3_storage_prefix_path = '/SingletonS3Storage'
    class SingletonS3StorageController(SingletonStorageController):
        def __init__(self, model:SingletonS3Storage):
            self.model:SingletonS3Storage = model
            self.bucket_name = self.model.bucket_name

        def _s3_path(self,key:str):
            return f'{self.model.s3_storage_prefix_path}/{key}.json'
            
        def _de_s3_path(self,path:str):
            return path.replace(f'{self.model.s3_storage_prefix_path}/',''
                         ).replace(f'.json','')
        
        def exists(self, key: str)->bool:
            try:
                self.model.s3.head_object(Bucket=self.bucket_name,
                                          Key=self._s3_path(key))
                return True
            except self.model.s3.exceptions.NoSuchKey:
                return False
            
        def set(self, key: str, value: dict):
            try:
                json_data = json.dumps(value)
                self.model.s3.put_object(Bucket=self.bucket_name,
                                         Key=self._s3_path(key), Body=json_data)
            except Exception as e:
                print(e)
        
        def get(self, key: str)->dict:
            try:
                obj = self.model.s3.get_object(Bucket=self.bucket_name, Key=self._s3_path(key))
                return json.loads(obj['Body'].read().decode('utf-8'))
            except self.model.s3.exceptions.NoSuchKey:
                return None
                
        def delete(self, key):
            try:
                self.model.s3.delete_object(Bucket=self.bucket_name, Key=self._s3_path(key))
            except self.model.s3.exceptions.NoSuchKey:
                print('NoSuchKey')
            
        def keys(self, pattern='*')->list[str]:
            keys = []
            paginator = self.model.s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket_name,
                                           Prefix=self.model.s3_storage_prefix_path):
                for obj in page.get('Contents', []):
                    keys.append(self._de_s3_path(obj['Key']))
                    
            return fnmatch.filter(keys, pattern)
       
if mongo_back:
    from pymongo import MongoClient, database, collection
    
    class SingletonMongoDBStorage:
        _instance = None
        _meta = {}
        
        def __new__(cls, mongo_URL: str = "mongodb://127.0.0.1:27017/", 
                        db_name: str = "SingletonDB", collection_name: str = "store"):            
            same_url = cls._meta.get('mongo_URL',None)==mongo_URL
            same_db = cls._meta.get('db_name',None)==db_name
            same_col = cls._meta.get('collection_name',None)==collection_name

            if not (same_url and same_db and same_col):
                cls._instance = None

            if cls._instance is None:
                cls._instance = super(SingletonMongoDBStorage, cls).__new__(cls)
                cls._instance.uuid = uuid.uuid4()
                client = MongoClient(mongo_URL)
                cls._instance.db = client.get_database(db_name)
                cls._instance.collection = cls._instance.db.get_collection(collection_name)
                cls._instance._meta = dict(mongo_URL=mongo_URL,db_name=db_name,collection_name=collection_name)
            return cls._instance

        def __init__(self, mongo_URL: str = "mongodb://127.0.0.1:27017/", 
                        db_name: str = "SingletonDB", collection_name: str = "store"):
            self.uuid: str = self.uuid
            self.db:database.Database = self.db
            self.collection:collection.Collection = self.collection

    class SingletonMongoDBStorageController(SingletonStorageController):
        
        def __init__(self, model: SingletonMongoDBStorage):
            self.model: SingletonMongoDBStorage = model

        def _ID_KEY(self):return '_id'

        def exists(self, key: str)->bool:
            return self.model.collection.find_one({self._ID_KEY(): key}) is not None

        def set(self, key: str, value: dict):
            self.model.collection.update_one({self._ID_KEY(): key}, {"$set": value}, upsert=True)

        def get(self, key: str)->dict:
            res = self.model.collection.find_one({self._ID_KEY(): key})            
            if res: del res['_id']
            return res

        def delete(self, key: str):
            self.model.collection.delete_one({self._ID_KEY(): key})

        def keys(self, pattern: str = '*')->list[str]:
            regex = '^'+pattern.replace('*', '.*')
            return [doc['_id'] for doc in self.model.collection.find({self._ID_KEY(): {"$regex": regex}})]

class EventDispatcherController:
    ROOT_KEY = 'Event'

    def __init__(self, client=None):
        if client is None:
            client = SingletonPythonDictStorageController(PythonDictStorage())
        self.client:SingletonStorageController = client
    
    def events(self):
        return list(zip(self.client.keys('*'),[self.client.get(k) for k in self.client.keys('*')]))

    def _find_event(self, uuid: str):
        es = self.client.keys(f'*:{uuid}')
        return [None] if len(es)==0 else es
    
    def get_event(self, uuid: str):
        return [self.client.get(k) for k in self._find_event(uuid)]
    
    def delete_event(self, uuid: str):
        return [self.client.delete(k) for k in self._find_event(uuid)]
    
    def set_event(self, event_name: str, callback, id:str=None):
        if id is None:id = uuid.uuid4()
        self.client.set(f'{EventDispatcherController.ROOT_KEY}:{event_name}:{id}', callback)
        return id
    
    def dispatch(self, event_name, *args, **kwargs):
        for event_full_uuid in self.client.keys(f'{EventDispatcherController.ROOT_KEY}:{event_name}:*'):
            self.client.get(event_full_uuid)(*args, **kwargs)

    def clean(self):
        return self.client.clean()
    
class KeysHistoryController:
    def __init__(self, client=None):
        if client is None:
            client = SingletonPythonDictStorageController(PythonDictStorage())
        self.client:SingletonStorageController = client

    def _str2base64(self,key: str):
        return base64.b64encode(key.encode()).decode()
    def reset(self):
        self.client = SingletonPythonDictStorageController(PythonDictStorage())        
    def set_history(self,key: str, result:dict):
        if result:
            self.client.set(f'_History:{self._str2base64(key)}',{'result':result})
        return result
    
    def get_history(self,key: str):
        res = self.client.get(f'_History:{self._str2base64(key)}')
        return res.get('result',None) if res else None

    def try_history(self,key: str, result_func=lambda :None):
        res = self.get_history(key)
        if res is None:
            res = result_func()
            if res : self.set_history(key,res)
        return res

class LocalVersionController:
    def __init__(self, client=None):
        if client is None:
            client = SingletonPythonDictStorageController(PythonDictStorage())
        self.client:SingletonStorageController = client

    def _str2base64(self,key: str):
        return base64.b64encode(key.encode()).decode()
    
    def add_operation(self,operation:dict):
        ophash = self._str2base64(json.dumps(operation))
        self.client.set(f'_Operation:{ophash}',{'forward':operation,'revert':None})
        ops = self.client.get(f'_Operations')
        ops.append(ophash)
        self.client.set(f'_Operations',ops)
    
    def revert_one_operation(self):
        ops = self.client.get(f'_Operations')
        ophash = ops[-1]
        op = self.client.get(f'_Operation:{ophash}')
        revert = op['revert']
        
        # do revert

        ops.pop()
        self.client.set(f'_Operations',ops)

    def revert_operations_untill(self,ophash):
        pass

class SingletonKeyValueStorage(SingletonStorageController):

    def __init__(self)->None:
        self.conn:SingletonStorageController = None
        self.python_backend()
    
    def _switch_backend(self,name:str='python',*args,**kwargs):
        self.event_dispa = EventDispatcherController()
        self._hist = KeysHistoryController()
        backs={
            'python':lambda:SingletonPythonDictStorageController(SingletonPythonDictStorage(*args,**kwargs)),
            'firestore':lambda:SingletonFirestoreStorageController(SingletonFirestoreStorage(*args,**kwargs)) if firestore_back else None,
            'redis':lambda:SingletonRedisStorageController(SingletonRedisStorage(*args,**kwargs)) if redis_back else None,
            'sqlite':lambda:SingletonSqliteStorageController(SingletonSqliteStorage(*args,**kwargs)) if sqlite_back else None,
            'mongodb':lambda:SingletonMongoDBStorageController(SingletonMongoDBStorage(*args,**kwargs)) if mongo_back else None,
            's3':lambda:SingletonS3StorageController(SingletonS3Storage(*args,**kwargs)) if aws_s3 else None,
        }
        back=backs.get(name.lower(),lambda:None)()
        if back is None:raise ValueError(f'no back end of {name}, has {list(backs.items())}')
        return back
    
    def s3_backend(self,bucket_name,
                    aws_access_key_id,aws_secret_access_key,region_name,
                    s3_storage_prefix_path = '/SingletonS3Storage'):
        self.conn = self._switch_backend('s3',bucket_name,
                    aws_access_key_id,aws_secret_access_key,region_name,
                    s3_storage_prefix_path = s3_storage_prefix_path)

    def python_backend(self):
        self.conn = self._switch_backend('python')
    
    def sqlite_backend(self):             
        self.conn = self._switch_backend('sqlite')

    def firestore_backend(self,google_project_id:str=None,google_firestore_collection:str=None):
        self.conn = self._switch_backend('firestore',google_project_id,google_firestore_collection)

    def redis_backend(self,redis_URL:str='redis://127.0.0.1:6379'):
        self.conn = self._switch_backend('redis',redis_URL)

    def mongo_backend(self,mongo_URL:str="mongodb://127.0.0.1:27017/",
                        db_name:str="SingletonDB", collection_name:str="store"):
        self.conn = self._switch_backend('mongodb',mongo_URL,db_name,collection_name)

    def _print(self,msg):
        print(f'[{self.__class__.__name__}]: {msg}')

    def add_slave(self, slave:object, event_names=['set','delete'])->bool:
        if getattr(slave,'uuid',None) is None:
            try:
                setattr(slave,'uuid',uuid.uuid4())
            except Exception:
                self._print(f'can not set uuid to {slave}. Skip this slave.')
                return
        for m in event_names:
            if hasattr(slave, m):
                self.event_dispa.set_event(m,getattr(slave,m),getattr(slave,'uuid'))
            else:
                self._print(f'no func of "{m}" in {slave}. Skip it.')
                
    def delete_slave(self, slave:object)->bool:
        self.event_dispa.delete_event(getattr(slave,'uuid',None))

    def _edit(self,func_name:str, key:str=None, value:dict=None):
        if func_name not in ['set','delete','clean','load','loads']:
            self._print(f'no func of "{func_name}". return.')
            return
        self._hist.reset()
        func = getattr(self.conn, func_name)
        args = list(filter(lambda x:x is not None, [key,value]))
        res = func(*args)
        self.event_dispa.dispatch(func_name,*args)
        return res
    
    def _try_if_error(self,func):
        res = False
        try:
            func()
            res =  True
        except Exception as e:
            self._print(e)
            return False
        if res:
            pass
        return res
    # True False(in error)
    def set(self, key: str, value: dict):     return self._try_if_error(lambda:self._edit('set',key,value))
    def delete(self, key: str):               return self._try_if_error(lambda:self._edit('delete',key))
    def clean(self):                          return self._try_if_error(lambda:self._edit('clean'))
    def load(self,json_path):                 return self._try_if_error(lambda:self._edit('load', json_path))
    def loads(self,json_str):                 return self._try_if_error(lambda:self._edit('loads',json_str))
    
    def _try_obj_error(self,func):
        try:
            return func()
        except Exception as e:
            self._print(e)
            return None
    # Object, None(in error)
    # def exists(self, key: str)->bool:         return self._try_obj_error(lambda:self._hist.try_history(key,  lambda:self.conn.exists(key)))
    # def keys(self, regx: str='*')->list[str]: return self._try_obj_error(lambda:self._hist.try_history(regx, lambda:self.conn.keys(regx)))
    def exists(self, key: str)->bool:         return self._try_obj_error(lambda:self.conn.exists(key))
    def keys(self, regx: str='*')->list[str]: return self._try_obj_error(lambda:self.conn.keys(regx))
    def get(self, key: str)->dict:            return self._try_obj_error(lambda:self.conn.get(key))
    def dumps(self)->str:                     return self._try_obj_error(lambda:self.conn.dumps())
    def dump(self,json_path):                 return self._try_obj_error(lambda:self.conn.dump(json_path))

class Tests(unittest.TestCase):
    def __init__(self,*args,**kwargs)->None:
        super().__init__(*args,**kwargs)
        self.store = SingletonKeyValueStorage()

    def test_all(self,num=1):
        self.test_python(num)
        self.test_sqlite(num)
        # self.test_mongo(num)
        # self.test_redis(num)
        # self.test_firestore(num)

    def test_python(self,num=1):
        self.store.python_backend()
        for i in range(num):self.test_all_cases()

    def test_redis(self,num=1):
        self.store.redis_backend()
        for i in range(num):self.test_all_cases()

    def test_sqlite(self,num=1):
        self.store.sqlite_backend()
        for i in range(num):self.test_all_cases()

    def test_firestore(self,num=1):
        self.store.firestore_backend()
        for i in range(num):self.test_all_cases()

    def test_mongo(self,num=1):
        self.store.mongo_backend()
        for i in range(num):self.test_all_cases()

    def test_s3(self,num=1):
        self.store.s3_backend(
                    bucket_name = os.environ['AWS_S3_BUCKET_NAME'],
                    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                    region_name=os.environ['AWS_DEFAULT_REGION']
                )
        for i in range(num):self.test_all_cases()

    def test_all_cases(self):
        self.test_set_and_get()
        self.test_exists()
        self.test_delete()
        self.test_keys()
        self.test_get_nonexistent()
        self.test_dump_and_load()
        self.test_slaves()

    def test_set_and_get(self):
        self.store.set('test1', {'data': 123})
        self.assertEqual(self.store.get('test1'), {'data': 123}, "The retrieved value should match the set value.")

    def test_exists(self):
        self.store.set('test2', {'data': 456})
        self.assertTrue(self.store.exists('test2'), "Key should exist after being set.")

    def test_delete(self):
        self.store.set('test3', {'data': 789})
        self.store.delete('test3')
        self.assertFalse(self.store.exists('test3'), "Key should not exist after being deleted.")

    def test_keys(self):
        self.store.set('alpha', {'info': 'first'})
        self.store.set('abeta', {'info': 'second'})
        self.store.set('gamma', {'info': 'third'})
        expected_keys = ['alpha', 'abeta']
        self.assertEqual(sorted(self.store.keys('a*')), sorted(expected_keys), 
                         "Should return the correct keys matching the pattern.")

    def test_get_nonexistent(self):
        self.assertEqual(self.store.get('nonexistent'), None, "Getting a non-existent key should return None.")
        
    def test_dump_and_load(self):
        raw = {"test1": {"data": 123}, "test2": {"data": 456}, "alpha": {"info": "first"}, 
               "abeta": {"info": "second"}, "gamma": {"info": "third"}}
        self.store.dump('test.json')

        self.store.clean()
        self.assertEqual(self.store.dumps(),'{}', "Should return the correct keys and values.")

        self.store.load('test.json')
        self.assertEqual(json.loads(self.store.dumps()),raw, "Should return the correct keys and values.")
        
        self.store.clean()
        self.store.loads(json.dumps(raw))
        self.assertEqual(json.loads(self.store.dumps()),raw, "Should return the correct keys and values.")

    def test_slaves(self):
        if self.store.conn.__class__.__name__=='SingletonPythonDictStorageController':return
        store2 = SingletonKeyValueStorage()
        self.store.add_slave(store2)
        self.store.set('alpha', {'info': 'first'})
        self.store.set('abeta', {'info': 'second'})
        self.store.set('gamma', {'info': 'third'})
        self.store.delete('abeta')
        self.assertEqual(json.loads(self.store.dumps()),json.loads(store2.dumps()), "Should return the correct keys and values.")

#############################################################################################
#############################################################################################
#############################################################################################

def now_utc():
    return datetime.now().replace(tzinfo=ZoneInfo("UTC"))

class Controller4Basic:
    class AbstractObjController:
        def __init__(self, store, model):
            self.model:Model4Basic.AbstractObj = model
            self._store:BasicStore = store

        def update(self, **kwargs):
            assert  self.model is not None, 'controller has null model!'
            for key, value in kwargs.items():
                if hasattr(self.model, key):
                    setattr(self.model, key, value)
            self._update_timestamp()
            self.store()

        def _update_timestamp(self):
            assert  self.model is not None, 'controller has null model!'
            self.model.update_time = now_utc()
            
        def store(self):
            assert self.model._id is not None
            self._store.set(self.model._id,self.model.model_dump_json_dict())
            return self

        def delete(self):
            self._store.delete(self.model.get_id())
            self.model._controller = None

        def update_metadata(self, key, value):
            updated_metadata = {**self.model.metadata, key: value}
            self.update(metadata = updated_metadata)
            return self
        
class Model4Basic:
    class AbstractObj(BaseModel):
        _id: str=None
        rank: list = [0]
        create_time: datetime = Field(default_factory=now_utc)
        update_time: datetime = Field(default_factory=now_utc)
        status: str = ""
        metadata: dict = {}

        def model_dump_json_dict(self):
            return json.loads(self.model_dump_json())

        def class_name(self): return self.__class__.__name__

        def set_id(self,id:str):
            assert self._id is None, 'this obj is been setted! can not set again!'
            self._id = id
            return self
        
        def gen_new_id(self): return f"{self.class_name()}:{uuid4()}"

        def get_id(self):
            assert self._id is not None, 'this obj is not setted!'
            return self._id
        
        model_config = ConfigDict(arbitrary_types_allowed=True)    
        _controller: Controller4Basic.AbstractObjController = None
        def get_controller(self)->Controller4Basic.AbstractObjController: return self._controller
        def init_controller(self,store):self._controller = Controller4Basic.AbstractObjController(store,self)

class BasicStore(SingletonKeyValueStorage):
    
    def __init__(self) -> None:
        self.python_backend()

    def _get_class(self, id: str, modelclass=Model4Basic):
        class_type = id.split(':')[0]
        res = {c.__name__:c for c in [i for k,i in modelclass.__dict__.items() if '_' not in k]}
        res = res.get(class_type, None)
        if res is None: raise ValueError(f'No such class of {class_type}')
        return res
    
    def _get_as_obj(self,id,data_dict)->Model4Basic.AbstractObj:
        obj:Model4Basic.AbstractObj = self._get_class(id)(**data_dict)
        obj.set_id(id).init_controller(self)
        return obj
    
    
    def _add_new_obj(self, obj:Model4Basic.AbstractObj, id:str=None):
        id,d = obj.gen_new_id() if id is None else id, obj.model_dump_json_dict()
        self.set(id,d)
        return self._get_as_obj(id,d)
    
    def add_new_obj(self, obj:Model4Basic.AbstractObj, id:str=None):        
        if obj._id is not None: raise ValueError(f'obj._id is {obj._id}, must be none')
        return self._add_new_obj(obj,id)
    
    # available for regx?
    def find(self,id:str) -> Model4Basic.AbstractObj:
        raw = self.get(id)
        if raw is None:return None
        return self._get_as_obj(id,raw)
    
    def find_all(self,id:str=f'AbstractObj:*')->list[Model4Basic.AbstractObj]:
        return [self.find(k) for k in self.keys(id)]

class Tests(unittest.TestCase):
    def __init__(self,*args,**kwargs)->None:
        super().__init__(*args,**kwargs)
        self.store = BasicStore()

    def test_all(self,num=1):
        self.test_python(num)

    def test_python(self,num=1):
        self.store.python_backend()
        for i in range(num):self.test_all_cases()
        self.store.clean()

    def test_all_cases(self):
        self.store.clean()
        self.test_add_and_get()
        self.test_find_all()
        # self.test_exists()
        self.test_delete()
        self.test_get_nonexistent()
        self.test_dump_and_load()
        # self.test_slaves()

    def test_get_nonexistent(self):
        self.assertEqual(self.store.find('nonexistent'), None, "Getting a non-existent key should return None.")
        
    def test_add_and_get(self):
        obj = self.store.add_new_obj(Model4Basic.AbstractObj())
        objr = self.store.find(obj.get_id())
        self.assertEqual(obj.model_dump_json_dict(),
                        objr.model_dump_json_dict(),
                         "The retrieved value should match the set value.")
    def test_find_all(self):
        self.store.add_new_obj(Model4Basic.AbstractObj())
        self.assertEqual(len(self.store.find_all()),2,
                         "The retrieved value should match number of objs.")

    def test_dump_and_load(self):
        a = self.store.find_all()
        js = self.store.dumps()
        self.store.clean()
        self.store.loads(js)
        b = self.store.find_all()
        self.assertTrue(all([x.model_dump_json_dict()==y.model_dump_json_dict() for x,y in zip(a,b)]),
                         "The same before dumps and loads.")

    def test_delete(self):
        obj = self.store.find_all()[0]
        obj.get_controller().delete()
        self.assertFalse(self.store.exists(obj.get_id()), "Key should not exist after being deleted.")

#############################################################################################
#############################################################################################
#############################################################################################

class Controller4LLM:
    class AbstractObjController(Controller4Basic.AbstractObjController):
        pass

    class CommonDataController(AbstractObjController):
        def __init__(self, store, model):
            self.model: Model4LLM.CommonData = model
            self._store:LLMstore = store

    class AuthorController(AbstractObjController):
        def __init__(self, store ,model):
            self.model: Model4LLM.Author = model
            self._store:LLMstore = store

    class AbstractContentController(AbstractObjController):
        def __init__(self, store, model):
            self.model: Model4LLM.AbstractContent = model
            self._store:LLMstore = store

        def delete(self):
            self.get_data()._controller.delete()        
            # self._store.delete_obj(self.model)        
            self._store.delete(self.model.get_id())
            self.model._controller = None

        def get_author(self):
            author:Model4LLM.Author = self._store.find(self.model.author_id)
            return author

        def get_group(self):
            res:Model4LLM.ContentGroup = self._store.find(self.model.group_id)
            return res
        
        def get_data(self):
            res:Model4LLM.CommonData = self._store.find(self.model.data_id())
            return res

        def get_data_raw(self):
            return self.get_data().raw    

        def update_data_raw(self, msg: str):
            self.get_data()._controller.update(raw = msg)
            return self

        def append_data_raw(self, msg: str):
            data = self.get_data()
            data._controller.update(raw = data.raw + msg)
            return self
        
    class AbstractGroupController(AbstractObjController):
        def __init__(self, store, model):
            self.model:Model4LLM.AbstractGroup = model
            self._store:LLMstore = store

        def yield_children_content_recursive(self, depth: int = 0):
            for child_id in self.model.children_id:
                if not self._store.exists(child_id):
                    continue
                content:Model4LLM.AbstractObj = self._store.find(child_id)
                yield content, depth
                if child_id.startswith('ContentGroup'):
                    group:Controller4LLM.AbstractGroupController = content._controller
                    for cc, d in group.yield_children_content_recursive(depth + 1):
                        yield cc, d

        def delete_recursive_from_keyValue_storage(self):
            for c, d in self.yield_children_content_recursive():
                c._controller.delete()
            self.delete()

        def get_children_content(self):
            # self.load()
            assert  self.model is not None, 'controller has null model!'
            results:List[Model4LLM.AbstractObj] = []
            for child_id in self.model.children_id:
                results.append(self._store.find(child_id))
            return results

        def get_child_content(self, child_id: str):
            res:Model4LLM.AbstractContent = self._store.find(child_id)
            return res

        def prints(self):
            res = '########################################################\n'
            for content, depth in self.yield_children_content_recursive():
                res += f"{'    ' * depth}{content.get_id()}\n"
            res += '########################################################\n'
            print(res)
            return res

    class ContentGroupController(AbstractGroupController):
        def __init__(self, store, model):
            self.model:Model4LLM.ContentGroup = model
            self._store:LLMstore = store

        def add_new_child_group(self,metadata={},rank=[0]):
            parent,child = self._store.add_new_group_to_group(group=self.model,metadata=metadata,rank=rank)
            return child

        def add_new_text_content(self, author_id:str, text:str):
            parent,child = self._store.add_new_text_to_group(group=self.model,author_id=author_id,
                                                    text=text)                             
            return child
        
        def add_new_embeding_content(self, author_id:str, content_id:str, vec:list[float]):
            parent,child = self._store.add_new_embedding_to_group(group=self.model,author_id=author_id,
                                                        content_id=content_id, vec=vec)                                   
            return child
        
        def add_new_image_content(self,author_id:str, filepath:str):
            parent,child = self._store.add_new_image_to_group(group=self.model,author_id=author_id,
                                                    filepath=filepath)                              
            return child
            

        def remove_child(self, child_id:str):
            remaining_ids = [cid for cid in self.model.children_id if cid != child_id]
            for content in self.get_children_content():
                if content._controller.model.get_id() == child_id:
                    if child_id.startswith('ContentGroup'):
                        group:Controller4LLM.ContentGroupController = content._controller
                        group.delete_recursive_from_keyValue_storage()
                    content._controller.delete()
                    break
            self.update(children_id = remaining_ids)
            return self

        def get_children_content_recursive(self):
            results:list[Model4LLM.AbstractContent] = []
            for c, d in self.yield_children_content_recursive():
                results.append(c)
            return results

    class TextContentController(AbstractContentController):
        def __init__(self, store, model):
            self.model:Model4LLM.TextContent = model
            self._store:LLMstore = store


    class EmbeddingContentController(AbstractContentController):
        def __init__(self, store, model):
            self.model: Model4LLM.EmbeddingContent = model
            self._store:LLMstore = store

        def get_data_raw(self):
            return list(map(float,super().get_data_raw()[1:-1].split(',')))
        
        def get_data_rLOD0(self):
            return self.get_data_raw()[::10**(0+1)]
        
        def get_data_rLOD1(self):
            return self.get_data_raw()[::10**(1+1)]
        
        def get_data_rLOD2(self):
            return self.get_data_raw()[::10**(2+1)]
        
        def get_target(self):
            assert  self.model is not None, 'controller has null model!'
            target_id = self.model.target_id
            res:Model4LLM.AbstractContent = self._store.find(target_id)
            return res
        
        def update_data_raw(self, embedding: list[float]):
            super().update_data_raw(str(embedding))
            return self

    class FileLinkContentController(AbstractContentController):
        def __init__(self, store, model):
            self.model: Model4LLM.FileLinkContent = model
            self._store:LLMstore = store

    class BinaryFileContentController(AbstractContentController):
        def __init__(self, store, model):
            self.model: Model4LLM.BinaryFileContent = model
            self._store:LLMstore = store
            
        def read_bytes(self, filepath):
            with open(filepath, "rb") as f:
                return f.read()
            
        def b64decode(self, file_base64):
            return base64.b64decode(file_base64)
            
        def get_data_rLOD0(self):
            raise ValueError('binary file has no LOD concept')
        
        def get_data_rLOD1(self):
            raise ValueError('binary file has no LOD concept')
        
        def get_data_rLOD2(self):
            raise ValueError('binary file has no LOD concept')
        
    class ImageContentController(BinaryFileContentController):
        def __init__(self, store, model):
            self.model: Model4LLM.ImageContent = model    
            self._store:LLMstore = store

        def decode_image(self, encoded_string):
            return Image.open(io.BytesIO(self.b64decode(encoded_string)))
        
        def get_image(self):
            encoded_image = self.get_data_raw()
            if encoded_image:
                image = self.decode_image(encoded_image)
                return image
            return None
                    
        def get_image_format(self):
            image = self.get_image()
            return image.format if image else None
        
        def get_data_rLOD(self,lod=0):
            image = self.get_image()
            ratio = 10**(lod+1)
            if image.size[0]//ratio==0 or image.size[1]//ratio ==0:
                raise ValueError(f'img size({image.size}) of LOD{lod} is smaller than 0')
            return image.resize((image.size[0]//ratio,image.size[1]//ratio)) if image else None

        def get_data_rLOD0(self):
            return self.get_data_rLOD(lod=0)
        
        def get_data_rLOD1(self):
            return self.get_data_rLOD(lod=1)
        
        def get_data_rLOD2(self):
            return self.get_data_rLOD(lod=2)
    
class Model4LLM:
    class AbstractObj(Model4Basic.AbstractObj):
        pass

    class CommonData(AbstractObj):
        raw: str = ''
        rLOD0: str = ''
        rLOD1: str = ''
        rLOD2: str = ''
        _controller: Controller4LLM.CommonDataController = None
        
        def get_controller(self)->Controller4LLM.CommonDataController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.CommonDataController(store,self)
    class Author(AbstractObj):
        name: str = ''
        role: str = ''
        _controller: Controller4LLM.AuthorController = None
        
        def get_controller(self)->Controller4LLM.AuthorController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.AuthorController(store,self)
    class AbstractContent(AbstractObj):
        author_id: str=''
        group_id: str=''
        _controller: Controller4LLM.AbstractContentController = None
        
        def get_controller(self)->Controller4LLM.AbstractContentController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.AbstractContentController(store,self)

        def data_id(self):return f"CommonData:{self.get_id()}"
    class AbstractGroup(AbstractObj):
        author_id: str=''
        parent_id: str = ''
        children_id: List[str] = []
        _controller: Controller4LLM.AbstractGroupController = None
        
        def get_controller(self)->Controller4LLM.AbstractGroupController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.AbstractGroupController(store,self)
    class ContentGroup(AbstractGroup):
        _controller: Controller4LLM.ContentGroupController = None
        
        def get_controller(self)->Controller4LLM.ContentGroupController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.ContentGroupController(store,self)
    class TextContent(AbstractContent):
        _controller: Controller4LLM.TextContentController = None
        
        def get_controller(self)->Controller4LLM.TextContentController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.TextContentController(store,self)

    class EmbeddingContent(AbstractContent):
        _controller: Controller4LLM.EmbeddingContentController = None
        
        def get_controller(self)->Controller4LLM.EmbeddingContentController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.EmbeddingContentController(store,self)
        target_id: str
    class FileLinkContent(AbstractContent):
        _controller: Controller4LLM.FileLinkContentController = None
        
        def get_controller(self)->Controller4LLM.FileLinkContentController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.FileLinkContentController(store,self)
    class BinaryFileContent(AbstractContent):
        _controller: Controller4LLM.BinaryFileContentController = None
        
        def get_controller(self)->Controller4LLM.BinaryFileContentController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.BinaryFileContentController(store,self)
            
    class ImageContent(BinaryFileContent):
        _controller: Controller4LLM.ImageContentController = None
        
        def get_controller(self)->Controller4LLM.ImageContentController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.ImageContentController(store,self)

class LLMstore(BasicStore):
    
    def __init__(self) -> None:
        self.python_backend()

    def _get_class(self, id: str, modelclass=Model4LLM):
        return super()._get_class(id, modelclass)
    
    def add_new_author(self,name, role, rank:list=[0], metadata={}) -> Model4LLM.Author:
        return self.add_new_obj(Model4LLM.Author(name=name, role=role, rank=rank, metadata=metadata))
    
    def add_new_root_group(self,metadata={},rank=[0]) -> Model4LLM.ContentGroup:
        return self.add_new_obj( Model4LLM.ContentGroup(rank=rank, metadata=metadata) )
    
    def _add_new_content_to_group(self,group:Model4LLM.ContentGroup,content:Model4LLM.AbstractContent,raw:str=None):
        if group._id is None:raise ValueError('the group is no exits!')
        content = self.add_new_obj(content)
        group.get_controller().update(children_id=group.children_id+[content.get_id()])
        if raw is not None and 'ContentGroup' not in content.get_id():
            self.add_new_obj(Model4LLM.CommonData(raw=raw),id=content.data_id())
        content.init_controller(self)
        return group,content    

    def read_image(self, filepath):
        with open(filepath, "rb") as f:
            return f.read()
        
    def b64encode(self, file_bytes):
        return base64.b64encode(file_bytes)
        
    def encode_image(self, image_bytes):
        return self.b64encode(image_bytes)
    
    def add_new_group_to_group(self,group:Model4LLM.ContentGroup,metadata={},rank=[0]):
        parent,child = self._add_new_content_to_group(group, Model4LLM.ContentGroup(rank=rank, metadata=metadata, parent_id=group.get_id()))
        return parent,child

    def add_new_text_to_group(self,group:Model4LLM.ContentGroup,author_id:str,text:str):
        parent,child = self._add_new_content_to_group(group,
                                                      Model4LLM.TextContent(author_id=author_id, group_id=group.get_id()),
                                                      raw=text)
        return parent,child
    
    def add_new_embedding_to_group(self,group:Model4LLM.ContentGroup, author_id:str, content_id:str, vec:list[float]):
        parent,child = self._add_new_content_to_group(group,
                                                      Model4LLM.EmbeddingContent(author_id=author_id, 
                                                                       group_id=group.get_id(),target_id=content_id),
                                                      raw=str(vec))
        return parent,child
    
    def add_new_image_to_group(self,group:Model4LLM.ContentGroup,author_id:str, filepath:str):
        raw_bytes = self.read_image(filepath)
        raw_base64 = self.encode_image(raw_bytes)
        parent,child = self._add_new_content_to_group(group,
                                                      Model4LLM.ImageContent(author_id=author_id,group_id=group.get_id()),
                                                      raw=raw_base64)
        return parent,child
    
    
    def find_group(self,id:str) -> Model4LLM.ContentGroup:
        assert 'Group:' in id, 'this id is not a id for goup'
        return self.find(id)
    
    def find_all_authors(self)->list[Model4LLM.Author]:
        return self.find_all('Author:*')

#############################################################################################
#############################################################################################
#############################################################################################

class Speaker:
    def __init__(self,author:Model4LLM.Author) -> None:
        self.author = author
        self.id = self.author.get_id()
        self.name = self.author.name#self.get_id()[-8:]
        self.room:ChatRoom = None
        self.is_speaking = 0
        self.new_message_callbacks=[]
        self.mention_callbacks=[]

    def add_new_message_callback(self, cb):
        self.new_message_callbacks.append(cb)
        return self

    def add_mention_callback(self, cb):
        self.mention_callbacks.append(cb)
        return self
        
    def on_new_message(self, message:Model4LLM.AbstractContent):
        for cb in self.new_message_callbacks:
            cb(self,message)

    def on_mention(self, message:Model4LLM.AbstractContent):
        for cb in self.mention_callbacks:
            cb(self,message)

    def entery_room(self,room):
        self.room:ChatRoom = room
        self.room.add_speaker(self)
        rooms = self.author.metadata.get('groups','')
        if self.room.chatroom().get_id() not in rooms:
            rooms += self.room.chatroom().get_id()
            self.author.get_controller().update_metadata('groups',rooms)
        return self
    
    def new_group(self):
        if self.room is None:
            raise ValueError('please enter room at first')
        groupid = self.room.add_content_group_to_chatroom()        
        return groupid

    def speak_img(self,imgpath:str,group_id:str=None,new_group=False):
        self.is_speaking += 1
        if self.room is not None:
            self.room.speak_img(self.id,imgpath,group_id,new_group)
        self.is_speaking -= 1
        return self

    def speak(self,msg:str,group_id:str=None,new_group=False):
        self.is_speaking += 1
        if self.room is not None:
            self.room.speak(self.id,msg,group_id,new_group)
        self.is_speaking -= 1
        return self
    
    def speak_stream(self,stream,group_id:str=None,new_group=False):
        self.is_speaking += 1
        def callback():
            self.is_speaking -= 1
        if self.room is not None:
            worker_thread = threading.Thread(target=self.room.speak_stream,args=(self.id,stream,callback,group_id,new_group))
            worker_thread.start()
        return self
    
class ChatRoom:
    def __init__(self, store:LLMstore, chatroom_id:str=None, speakers:Dict[str,Speaker]={}) -> None:
        self.store=store
        self.speakers=speakers
        chatroom:Model4LLM.ContentGroup = None
        roots:List[Model4LLM.ContentGroup] = self.store.find_all('ContentGroup:*')
        if chatroom_id is None:
            roots = [g for g in roots if len(g.parent_id)==0]
            if len(roots)==0:
                print(f'no group({chatroom_id}) in store, make a new one')
                roots = [self.store.add_new_root_group()]
        else:
            roots = [g for g in roots if g.get_id()==chatroom_id]
            if len(roots)==0:
                raise ValueError(f'no group({chatroom_id}) in store')        
        chatroom = roots[0]

        self.chatroom_id = chatroom.get_id()
        self.msgs = []
        
        for a in self.store.find_all_authors():
            if self.chatroom_id in a.metadata.get('groups',''):
                print(a)
                self.speakers[a.get_id()] = Speaker(a).entery_room(self)
                self.speakers[a.name] = self.speakers[a.get_id()]
        print(self.speakers)
    
    def chatroom(self):
        return self.store.find_group(self.chatroom_id)

    def _on_message_change(self):
        self.msgs = self.get_messages_in_group()
    
    def add_content_group_to_chatroom(self):
        child = self.chatroom().get_controller().add_new_child_group()
        self._on_message_change()
        return child.get_id()
    
    def get_messages_in_group(self,id=None)->List[Model4LLM.AbstractContent]:
        if id is None:
            return self.chatroom().get_controller().get_children_content()
        else:
            return self.store.find_group(id).get_controller().get_children_content()
    
    def get_messages_recursive_in_chatroom(self):
        return self.chatroom().get_controller().get_children_content_recursive()
    
    def traverse_nested_messages(self, nested_content_list=None):
        if nested_content_list is None:nested_content_list=self.get_messages_recursive_in_chatroom()
        for element in nested_content_list:
            if isinstance(element, list):
                for e in self.traverse_nested_messages(element):
                    yield e
            else:
                yield element
    
    ##################################################################

    def add_speaker(self,speaker:Speaker):
        self.speakers[speaker.id] = speaker
        self.speakers[speaker.name] = speaker

    def get_speaker(self,speaker_id) -> Speaker:
        if speaker_id not in self.speakers:
            raise ValueError(f'no such user {speaker_id}')
        return self.speakers[speaker_id]

    def get_mentions(self, message:Model4LLM.AbstractContent, speaker_ids=[]):
        msg_auther = self.speakers[message.author_id].name
        mentions = re.findall(r'@([a-zA-Z0-9]+)', message.get_controller().get_data_raw())
        targets = []
        
        for mention in mentions:
            for speaker in {self.get_speaker(s) for s in speaker_ids}:                
                if speaker.name == mention and msg_auther!=mention:
                    targets.append(speaker.id)
        return targets
    
    def notify_new_message(self, message:Model4LLM.AbstractContent, speaker_ids=[]):
        speaker_ids = set(speaker_ids)-set(self.get_mentions(message,speaker_ids))-set(self.speakers[message.author_id].id)
        ids = list(set([self.speakers[s] for s in speaker_ids]))
        ids = sorted(ids, key=lambda x:x.author.rank[0])
        for speaker in ids:
            speaker.on_new_message(message)
    
    def notify_mention(self, message:Model4LLM.AbstractContent, speaker_ids=[]):
        speaker_ids = self.get_mentions(message, speaker_ids)
        ids = list(set([self.speakers[s] for s in speaker_ids]))
        ids = sorted(ids, key=lambda x:x.author.rank[0])
        for speaker in ids:
            speaker.on_mention(message)
    
    def _prepare_speak(self,speaker_id,group_id:str=None,new_group=False,type='Text',msg=''):
        speaker = self.get_speaker(speaker_id)

        def add_content(obj:Model4LLM.ContentGroup,type=type):
            if 'Text' in type:
                return obj.get_controller().add_new_text_content
            elif 'Image' in type:
                return obj.get_controller().add_new_image_content
            else:
                raise ValueError(f'Unknown type of {type}')

        if (group_id is None and not new_group) or (group_id == self.chatroom_id):
            tc = add_content(self.chatroom())

        elif group_id is not None and not new_group:
            if group_id not in self.chatroom().children_id:
                raise ValueError(f'no such group {group_id}')
            group:Model4LLM.ContentGroup = self.store.find(group_id)
            tc = add_content(group)
            self._on_message_change()

        elif group_id is None and new_group:            
            group = self.chatroom().get_controller().add_new_child_group()
            tc = add_content(group)
        
        return tc(speaker.id, msg)

    def speak_stream(self,speaker_id,stream,callback,group_id:str=None,new_group=False):
        content:Model4LLM.AbstractContent = None
        msg = ''
        for i,r in enumerate(stream):
            assert r is not None, f'can not prepare string reply in speak_stream! {r}'
            if i==0:
                content = self._prepare_speak(speaker_id,group_id,new_group)
            content.get_controller().append_data_raw(r)
            msg += r
        callback()
        self.notify_new_message(content, self.speakers.keys())
        self.notify_mention(content, self.speakers.keys())

    def speak(self,speaker_id,msg:str,group_id:str=None,new_group=False):
        content = self._prepare_speak(speaker_id,group_id,new_group,msg=msg)
        self.notify_new_message(content, self.speakers.keys())
        self.notify_mention(content, self.speakers.keys())
        return content
    
    def speak_img(self,speaker_id,imagpath:str,group_id:str=None,new_group=False):
        content = self._prepare_speak(speaker_id,group_id,new_group,type='Image',msg=imagpath)
        self.notify_new_message(content, self.speakers.keys())
        self.notify_mention(content, self.speakers.keys())
        return content
    #######################################################################

    def msgsDict(self,refresh=False,msgs=None,todict=None):        
        if todict is None:
            def todict(v:Model4LLM.AbstractContent):
                c = v.get_controller()
                n = v.__class__.__name__
                if 'Text' in n:
                    return {"type": "text","text": c.get_data_raw()}
                if 'Image' in n:
                    return {"type": "image_url","image_url": {
                                "url": f"data:image/jpeg;base64,{c.get_data_raw()}"}}                
            # todict = lambda c:c.load().get_data_raw()
            
        if refresh:
            self.msgs:List[Model4LLM.AbstractContent] = self.get_messages_in_group()
        if msgs is None:
            msgs = self.msgs

        res = []
        for v in msgs:
            if 'ContentGroup' not in v.__class__.__name__:
                mc = v.get_controller()
                name = mc.get_author().name
                role = mc.get_author().role
                if 'EmbeddingContent' in v.__class__.__name__:
                    v:Model4LLM.EmbeddingContent = v
                    t = v.get_controller().get_target(
                        ).get_controller().get_data_raw()[:10]
                    # print(f'{intents}{self.speakers[m.model.author_id].name}: "{t}"=>{m.load().get_data_raw()[:5]}...')
                elif 'TextContent' in v.__class__.__name__:
                    res.append(dict(name=name,role=role,content=todict(v)))
                else:
                    res.append(dict(name=name,role=role,content=todict(v)))
            else:
                res.append(self.msgsDict(False,self.get_messages_in_group(v.get_id())))
        return res

    def printMsgs(self,refresh=False,intent=0,msgs:List[Model4LLM.AbstractContent]=None):
        if refresh:
            self.msgs:List[Model4LLM.AbstractContent] = self.get_messages_in_group()
        if msgs is None:
            msgs = self.msgs
        intents = "".join([' ']*intent)
        print("", flush=True)
        print(f'{intents}#############################################################')
        
        for i,v in enumerate(msgs):
            print(f'{intents}-------------------------------------------------------------')
            if 'ContentGroup' not in v.__class__.__name__:
                v:Model4LLM.AbstractContent = v
                if 'EmbeddingContent' in v.__class__.__name__:
                    v:Model4LLM.EmbeddingContent = v
                    econtroller = v.get_controller()
                    t = econtroller.get_target().get_controller().get_data_raw()[:10]
                    print(f'{intents}{self.speakers[econtroller.get_target().author_id].name}: "{t}"=>{econtroller.get_data_raw()[:5]}...')
                elif 'ImageContent' in v.__class__.__name__:
                    v:Model4LLM.ImageContent = v
                    im = v.get_controller().get_image()
                    print(f'{intents}{self.speakers[v.author_id].name}: Image{im.size} of {im.info}')
                else:
                    print(f'{intents}{self.speakers[v.author_id].name}: {v.get_controller().get_data_raw()}')
            else:
                self.printMsgs(False,intent+4,self.get_messages_in_group(v.get_id()))
        print(f'{intents}-------------------------------------------------------------')
        print(f'{intents}#############################################################')

#############################################################################################
#############################################################################################
#############################################################################################

store = SingletonKeyValueStorage()
# store.mongo_backend()

class RESTapi:
    api = FastAPI()
    class Item(BaseModel):
        key: str
        value: dict = None
    

    @api.get("/")
    def serve_html():
        return FileResponse("chat.html")
    
    @api.post("/store/set/")
    async def set_item(item: Item):
        return store.set(item.key, item.value)

    @api.get("/store/get/{key}")
    async def get_item(key: str):
        result = store.get(key)
        if result is None:
            raise HTTPException(status_code=404, detail="Item not found")
        return result

    @api.delete("/store/delete/{key}")
    async def delete_item(key: str):
        success = store.delete(key)
        if not success:
            raise HTTPException(status_code=404, detail="Item not found to delete")
        return {"deleted": key}
    
    @api.delete("/store/clean")
    async def delete_items():
        success = store.clean()
        if not success:
            raise HTTPException(status_code=404, detail="Item not found to delete")
        return {"clean": success}
    
    @api.get("/store/exists/{key}")
    async def exists_item(key: str):
        return {"exists": store.exists(key)}

    @api.get("/store/keys/{pattern}")
    async def get_keys(pattern: str = '*'):
        return store.keys(pattern)

    @api.post("/store/loads/")
    async def load_items(item_json: str):
        store.loads(item_json)
        return {"loaded": True}

    @api.get("/store/dumps/")
    async def dump_items():
        return store.dumps()
        