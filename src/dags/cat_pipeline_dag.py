from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import psycopg2
import os
import json

# --- CẤU HÌNH KẾT NỐI POSTGRESQL ---
DB_CONFIG = {
    'dbname': 'catdb',
    'user': 'postgres',
    'password': '123456',
    'host': 'localhost',
    'port': '5432'
}

DEFAULT_IMAGE_URL = "https://cdn2.thecatapi.com/images/aae.jpg"
IMAGE_DIR = "data/images"
os.makedirs(IMAGE_DIR, exist_ok=True)

# --- Tạo DAG ---
default_args = {
    'owner': 'airflow',
    'retries': 2,  # Thử lại 2 lần
    'retry_delay': timedelta(minutes=5),  # Thời gian delay giữa các lần thử lại
}

dag = DAG(
    dag_id = "cat_pipeline_dag",
    default_args=default_args,
    description='Pipeline xử lý mèo',
    schedule_interval='0 9 * * *',  # Chạy lúc 9h sáng mỗi ngày
    start_date=days_ago(1),
    catchup=False,
)

# --- Task 1: Crawl Cat ---
def crawl_cat():
    url = "https://api.thecatapi.com/v1/breeds"
    response = requests.get(url)
    if response.status_code == 200:
        cats = response.json()
        print(f"Có {len(cats)} giống mèo được crawl.")
        return cats
    else:
        raise Exception(f"Error: {response.status_code}")

# --- Task 2: Transform Cat ---
def transform_cat(raw_data):
    cleaned = []
    for cat in raw_data:
        temperament_raw = cat.get("temperament", "")
        temperament_list = [t.strip() for t in temperament_raw.split(",")] if temperament_raw else []
        image_url = cat.get("image", {}).get("url", DEFAULT_IMAGE_URL) or DEFAULT_IMAGE_URL
        cleaned.append({
            "id": cat.get("id"),
            "name": cat.get("name"),
            "origin": cat.get("origin"),
            "temperament": temperament_list,
            "life_span": cat.get("life_span"),
            "image_url": image_url
        })
    return cleaned

# --- Task 3: Save Cat ---
def save_cat(cleaned_data):
    # Kết nối PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Tạo bảng
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cat_breeds (
            id TEXT PRIMARY KEY,
            name TEXT,
            origin TEXT,
            temperament TEXT[],
            life_span TEXT,
            image_url TEXT
        )
    """)
    conn.commit()

    # Lưu dữ liệu và tải ảnh
    for cat in cleaned_data:
        try:
            # Lưu metadata vào DB
            cur.execute("""
                INSERT INTO cat_breeds (id, name, origin, temperament, life_span, image_url)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (
                cat['id'],
                cat['name'],
                cat['origin'],
                cat['temperament'],
                cat['life_span'],
                cat['image_url']
            ))
            conn.commit()

            # Tải ảnh về local
            image_path = os.path.join(IMAGE_DIR, f"{cat['id']}.jpg")
            if not os.path.exists(image_path):  # tránh tải trùng
                img_response = requests.get(cat['image_url'], timeout=10)
                if img_response.status_code == 200:
                    with open(image_path, "wb") as img_file:
                        img_file.write(img_response.content)
        except Exception as e:
            print(f"❌ Lỗi với mèo ID {cat['id']}: {e}")

    cur.close()
    conn.close()
    print("✅ Đã lưu dữ liệu và tải ảnh thành công.")

# --- Các task trong DAG ---
crawl_task = PythonOperator(
    task_id='crawl_cat',
    python_callable=crawl_cat,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_cat',
    python_callable=transform_cat,
    op_args=['{{ task_instance.xcom_pull(task_ids="crawl_cat") }}'],
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_cat',
    python_callable=save_cat,
    op_args=['{{ task_instance.xcom_pull(task_ids="transform_cat") }}'],
    dag=dag,
)

# --- Chạy DAG theo thứ tự ---
crawl_task >> transform_task >> save_task
