import pandas as pd
import numpy as np
from pymongo import MongoClient
from sqlalchemy import create_engine
import psycopg2
from psycopg2 import sql
import sys
import os

# Hàm để tạo database nếu chưa tồn tại
def create_database(dbname, user, password, host='localhost', port='5432'):
    try:
        # Kết nối đến database mặc định 'postgres'
        conn = psycopg2.connect(dbname='postgres', user=user, password=password, host=host, port=port)
        conn.autocommit = True  # Cho phép các lệnh như CREATE DATABASE không cần transaction
        cur = conn.cursor()

        # Kiểm tra xem database đã tồn tại chưa
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (dbname,))
        exists = cur.fetchone()

        if not exists:
            # Tạo database mới
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
            print(f"Database '{dbname}' đã được tạo thành công.")
        else:
            print(f"Database '{dbname}' đã tồn tại.")
        
        # Đóng kết nối
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Đã xảy ra lỗi khi tạo database: {e}")
        sys.exit(1)

# Hàm để cấp quyền cho người dùng trên schema public
def grant_permissions(user, dbname, superuser, superpassword, host='localhost', port='5432'):
    try:
        # Kết nối đến database 'nhatot_daxuly' với người dùng superuser
        conn = psycopg2.connect(dbname=dbname, user=superuser, password=superpassword, host=host, port=port)
        conn.autocommit = True
        cur = conn.cursor()

        # Cấp quyền USAGE và CREATE cho người dùng trên schema 'public'
        cur.execute(f"GRANT USAGE ON SCHEMA public TO {user};")
        cur.execute(f"GRANT CREATE ON SCHEMA public TO {user};")
        print(f"Đã cấp quyền USAGE và CREATE cho người dùng '{user}' trên schema 'public'.")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Đã xảy ra lỗi khi cấp quyền: {e}")
        sys.exit(1)

# 1. Kết nối đến MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['dbmybookbuy']  
collection = db['tblbookbuy']  

# 2. Lấy dữ liệu từ MongoDB
data = pd.DataFrame(list(collection.find()))

# 3. Hiển thị một vài dòng đầu tiên của dữ liệu
print("Dữ liệu ban đầu:")
print(data.head())

# 4. Xử lý dữ liệu
#xóa các dòng có dữ liệu bị thiếu
data.dropna(inplace=True)
# Chuyển đổi cột 'price' sang kiểu số
data['Market_Price'] = pd.to_numeric(data['Market_Price'], errors='coerce')
# Chuyển đổi cột 'price' sang kiểu số
data['Price'] = pd.to_numeric(data['Price'], errors='coerce')
# Chuyển đổi cột 'Num_Page' sang kiểu số
data['Num_Page'] = pd.to_numeric(data['Num_Page'], errors='coerce')

# Chuyển đổi cột 'Weight' sang kiểu số
data['Weight'] = pd.to_numeric(data['Weight'], errors='coerce')



# Chuyển đổi ObjectId thành chuỗi
data['_id'] = data['_id'].astype(str)

# 5. Xóa dữ liệu cũ trong collection 'nhatot_daxuly' để tránh lỗi duplicate key
db['dbmybookbuy1'].delete_many({})

# 6. Lưu dữ liệu đã xử lý vào MongoDB
processed_data = data.to_dict('records')  # Chuyển đổi DataFrame thành danh sách các dict
db['dbmybookbuy1'].insert_many(processed_data)  # Lưu vào một collection mới

print("\nDữ liệu đã được lưu vào collection 'mybookbuy_daxuly' trong MongoDB.")

# 7. Tạo database PostgreSQL nếu chưa tồn tại
postgres_user = 'postgres'          # Thay bằng tên người dùng PostgreSQL của bạn 
postgres_password = '1'   # Thay bằng mật khẩu PostgreSQL của bạn 
postgres_host = 'localhost'        # Thường là 'localhost' nếu chạy trên máy cục bộ
postgres_port = '5432'             # Cổng mặc định của PostgreSQL
postgres_db = 'dbmybookbuy1'      # Tên cơ sở dữ liệu PostgreSQL

# Cài đặt superuser để cấp quyền (thường là 'postgres')
superuser = 'postgres'             # Thay bằng tên người dùng superuser
superpassword = '1'       # Thay bằng mật khẩu của superuser

# Tạo cơ sở dữ liệu nếu chưa tồn tại
create_database(postgres_db, superuser, superpassword, postgres_host, postgres_port)

# 8. Cấp quyền cho người dùng 'huybui' trên schema 'public'
grant_permissions(postgres_user, postgres_db, superuser, superpassword, postgres_host, postgres_port)

# 9. Lưu dữ liệu vào PostgreSQL
# Tạo chuỗi kết nối
connection_string = f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}'

# Tạo engine SQLAlchemy
engine = create_engine(connection_string)

try:
    # Lưu DataFrame vào PostgreSQL
    data.to_sql('dbmybookbuy1', engine, if_exists='replace', index=False)
    print("\nDữ liệu đã được lưu vào PostgreSQL trong bảng 'dbmybookbuy1'.")
except Exception as e:
    print(f"Đã xảy ra lỗi khi lưu dữ liệu vào PostgreSQL: {e}")
