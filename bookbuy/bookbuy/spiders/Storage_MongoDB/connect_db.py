from pymongo import MongoClient
"""
    Kết nối tới cơ sở dữ liệu MongoDB và in ra 10 tài liệu đầu tiên từ collection 'products'.
    
    - Kết nối tới MongoDB tại địa chỉ 'localhost' và cổng mặc định '27017'.
    - Lựa chọn database 'mydatabase' và collection 'products'.
    - In ra thông báo khi kết nối thành công.
    - Kiểm tra và in ra số lượng tài liệu có trong collection:
        + Nếu có tài liệu, in ra 10 tài liệu đầu tiên.
        + Nếu không có tài liệu, in ra thông báo rằng collection rỗng.
    
    Trả về:
    - client: đối tượng MongoClient để thao tác với MongoDB.
    - collection: đối tượng collection 'products' trong database 'mydatabase'.
    
    Ngoại lệ:
    - Bắt lỗi chung và in thông báo lỗi nếu kết nối thất bại.
"""
def connect_mongodb():
    try:
        client = MongoClient('mongodb://localhost:27017/')
        
        db = client['mybookbuy']

        collection = db['book']

        print(f"Đã kết nối thành công tới database: {db.name}")

        document_count = collection.count_documents({})
        if document_count > 0:
            print(f"Collection có {document_count} tài liệu. In ra 10 dữ liệu đầu tiên:")
            
            for i, document in enumerate(collection.find().limit(10)):
                print(f"{i+1}. {document}")
        else:
            print("Collection rỗng.")

        return client, collection

    except Exception as e:
        print(f"Đã xảy ra lỗi khi kết nối tới MongoDB: {e}")
        return None, None

if __name__ == "__main__":
    client, collection = connect_mongodb()

    if client:
        client.close()
        print("Kết nối MongoDB đã được đóng.")
