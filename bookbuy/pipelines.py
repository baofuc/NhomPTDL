# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import scrapy
import pymongo
import json
# from bson.objectid import ObjectId
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import csv
import os
class MongoDBBookbuyPipeline:
    def __init__(self):
        
        self.client = pymongo.MongoClient('mongodb://localhost:27017')
        self.db = self.client['dbmybookbuy'] #Database      
        pass
    
    def process_item(self, item, spider):
        
        collection =self.db['tblbookbuy'] #Table
        try:
            collection.insert_one(dict(item))
            return item
        except Exception as e:
            raise DropItem(f"Error inserting item: {e}")       
        pass

class JsonDBBookbuyPipeline:
    def process_item(self, item, spider):
        with open('jsondatabookbuy.json', 'a', encoding='utf-8') as file:
            line = json.dumps(dict(item), ensure_ascii=False) + '\n'
            file.write(line)
        return item

class CSVDBBookbuyPipeline:
    
    def process_item(self, item, spider):
        with open('csvdatabookbuy.csv', 'a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file, delimiter='$')
            writer.writerow([
                item['Book_name'],
                item['Author'],
                item['Price'],
                item['Market_Price'],
                item['Status'],
                item['Publisher'],
                item['Issuiers'],
                item['Publish_date'],
                item['Num_Page'],
                item['Weight'],
                item['Content']
            ])
        return item
    pass
