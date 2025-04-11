# Bắt đầu từ image Python chính thức
FROM python:3.9-slim

# Cài đặt các thư viện hệ thống cần thiết
RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt các thư viện Python yêu cầu
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt

# Copy mã nguồn vào trong container
COPY . /app

# Cổng mà ứng dụng sẽ sử dụng
EXPOSE 8080

# Lệnh để chạy ứng dụng khi container khởi động
CMD ["python", "cat_pipeline.py"]
