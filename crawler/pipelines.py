# -*- coding: utf-8 -*-
# Definition of item pipelines
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
#import psycopg2
import datetime
import json
import pymongo
import kafka
from bson.json_util import dumps, loads
from scrapy.exceptions import DropItem


class MongoPipeline(object):
    """Pipeline for writing to a Mongo DB"""
    def __init__(self, mongo_uri, mongo_db, mongo_collection, publication_min_date):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection=mongo_collection
        self.publication_min_date=publication_min_date
        self.ids_seen = set()
        mongo_collection=pymongo.MongoClient(self.mongo_uri)[self.mongo_db].get_collection(name=self.mongo_collection).find({})
        for publication in mongo_collection:
            publication_id=publication['publication_id']
            self.ids_seen.add(publication_id)

    @classmethod
    def from_crawler(cls, crawler):
        mongo_settings=crawler.settings.get('MONGO')
        y, m, d = [int(t) for t in mongo_settings.get('publication_min_date').split('-')]
        return cls(
            mongo_uri=mongo_settings.get('uri'),
            mongo_db=mongo_settings.get('db'),
            mongo_collection=mongo_settings.get('collection'),
            publication_min_date=datetime.date(y,m,d).isoformat().encode('utf-8')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        if item['publication_id'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item)
        elif item['published']<self.publication_min_date:
            raise DropItem("Old publication found: %s" % item)
        else:
            self.ids_seen.add(item['publication_id'])
            self.db[self.mongo_collection].insert_one(dict(item))
        return item


#  IT NOT USED FOR THE MOMENT> IT SHOULD BE UPDATED BEFORE USE.
# class JsonWriterPipeline(object):
#     """Pipeline for writing to a file in JSON like notation"""
#     def __init__(self):
#         self.file = open('items.json', 'wb')
#
#     def process_item(self, item, spider):
#         line = json.dumps(dict(item)) + "\n"
#         self.file.write(line)
#         return item


class KafkaPipeline(object):
    """Pipeline for writing to a Mongo DB"""

    def __init__(self, kafka_server, kafka_topic, publication_min_date):
        self.kafka_server = kafka_server
        self.kafka_topic = kafka_topic
        self.publication_min_date=publication_min_date
        self.ids_seen=set()
        # Get already ids of already crawled publications
        kafka_consumer=kafka.KafkaConsumer(self.kafka_topic, auto_offset_reset='earliest', client_id='scrapy_project',bootstrap_servers=[self.kafka_server], api_version=(0, 10), consumer_timeout_ms=1000)
        for msg in kafka_consumer:
            publication_id=loads(msg.value)['publication_id']
            self.ids_seen.add(publication_id)


    @classmethod
    def from_crawler(cls, crawler):
        kafka_settings=crawler.settings.get('KAFKA')
        y, m, d = [int(t) for t in kafka_settings.get('publication_min_date').split('-')]
        return cls(
            kafka_server=kafka_settings.get('server'),
            kafka_topic=kafka_settings.get('topic'),
            publication_min_date = datetime.date(y, m, d).isoformat().encode('utf-8')
        )

    def open_spider(self, spider):
        self.producer = kafka.KafkaProducer(bootstrap_servers=[self.kafka_server], api_version=(0, 10))

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        if item['publication_id'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item)
        elif item['published']<self.publication_min_date:
            raise DropItem("Old publication found: %s" % item)
        else:
            self.ids_seen.add(item['publication_id'])
            key_bytes = item['publication_id'].encode("utf-8")
            value_bytes = dumps(item).encode("utf-8")
            self.producer.send(self.kafka_topic, key=key_bytes, value=value_bytes)
            self.producer.flush()
            return item


# class PostgresPipeline(object):
#     """Pipeline for writing to a PostgreSQL data base"""
#
#     def __init__(self, db_name, db_user, db_host, db_port, db_password):
#         """Initialize the data base"""
#         """Initialize the data base"""
#         try:
#             # Connect to the db using options set in settings.py
#             self.db = psycopg2.connect("dbname="+db_name+" user="+db_user+" host="+db_host+" port="+db_port+" password="+db_password)
#         except psycopg2.DatabaseError as e:
#             print(e)
#             exit(42)
#         self.db.autocommit = True
#         self.cursor = self.db.cursor()
#
#     @classmethod
#     def from_crawler(cls, crawler):
#         """Get data base options from settings.py"""
#         db_settings = crawler.settings.get('DATABASE')
#         return cls(
#             db_name = db_settings.get('database'),
#             db_user = db_settings.get('username'),
#             db_host = db_settings.get('host'),
#             db_port = db_settings.get('port'),
#             db_password = db_settings.get('password'),
#         )
#
#     def open_spider(self, spider):
#         """Initialize table when spider opens"""
#         self.tbl_name = spider.name
#         # Create a table with the same name as the spider if it does not exist already
#         self.cursor.execute(
#             "CREATE TABLE IF NOT EXISTS "+self.tbl_name+" (url text PRIMARY KEY, visited timestamp, published timestamp, title text, description text, text text, author text[], keywords text[]);")
#
#     def close_spider(self, spider):
#         """Close the connection when spider closes"""
#         self.db.commit()
#         self.db.close()
#
#     def process_item(self, item, spider):
#         """Process items and insert into data base"""
#         try:
#             # Needs postgresql version >= 9.5 for UPSERT, else remove "ON CONFLICT ..." line and handle duplicates
#             self.cursor.execute(
#                 "INSERT INTO "+self.tbl_name+" "+
#                     "VALUES (%s, %s, %s ,%s ,%s, %s, %s, %s) "+
#                     "ON CONFLICT DO NOTHING ;"
#                     ,(
#                     item['url'],
#                     item['visited'],
#                     item['published'],
#                     item['title'],
#                     item['description'],
#                     item['text'],
#                     item['author'],
#                     item['keywords'],
#                 )
#             )
#         except psycopg2.DatabaseError as e:
#             print(e)
#         return item
