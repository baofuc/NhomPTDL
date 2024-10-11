# Dockerfile cho ứng dụng Python
FROM python:3.9-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app/spiders/Storage_MongoDB

# Copy file requirements.txt vào container
COPY requirements.txt /app/spiders/requirements.txt

# Cài đặt các thư viện từ requirements.txt
RUN pip install --no-cache-dir -r /app/spiders/requirements.txt

# Copy toàn bộ mã nguồn của ứng dụng vào container
COPY . /app/spiders

# Chạy lệnh khởi động ứng dụng
CMD ["python", "pushdata.py"]