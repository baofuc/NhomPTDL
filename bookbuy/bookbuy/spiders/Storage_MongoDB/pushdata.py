import json
from pymongo import MongoClient
from pathlib import Path
import os

"""
    Script để chèn dữ liệu từ file JSON vào MongoDB.

    - Kết nối tới MongoDB tại địa chỉ 'localhost:27017'.
    - Đọc dữ liệu từ file JSON.
    - Chèn dữ liệu vào collection 'products' trong database 'mydatabase'.
"""

client = MongoClient('mongodb://mongodb:27017/')

db = client['dbmybookbuy']
collection = db['book']

relative_path = Path(os.path.join(os.getcwd(), '..', 'jsondatabookbuy.json'))

if not relative_path.is_file():
    print(f"File không tồn tại: {relative_path}")
else:
    with open(relative_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

    print("Dữ liệu đã được chèn vào MongoDB thành công!")
