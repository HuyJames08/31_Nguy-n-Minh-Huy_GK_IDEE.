import psycopg2
import json
import os
import requests

# --- Kết nối PostgreSQL ---
def connect_db():
    return psycopg2.connect(
        dbname="airflow",      # đổi nếu bạn dùng tên DB khác
        user="airflow",
        password="airflow", # đổi nếu bạn có mật khẩu khác
        host="localhost",
        port=5432
    )

# --- Tạo bảng nếu chưa có ---
def create_table(cur):
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

# --- Lưu dữ liệu vào bảng ---
def save_to_postgres(cur, cat_data):
    for cat in cat_data:
        cur.execute("""
            INSERT INTO cat_breeds (id, name, origin, temperament, life_span, image_url)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (
            cat["id"],
            cat["name"],
            cat["origin"],
            cat["temperament"],
            cat["life_span"],
            cat["image_url"]
        ))

# --- Tải ảnh về local ---
def download_images(cat_data):
    os.makedirs("data/images", exist_ok=True)

    for cat in cat_data:
        image_url = cat["image_url"]
        image_path = f"data/images/{cat['id']}.jpg"

        try:
            response = requests.get(image_url, timeout=10)
            if response.status_code == 200:
                with open(image_path, "wb") as f:
                    f.write(response.content)
                print(f"🖼️ Tải xong: {image_path}")
            else:
                print(f"⚠️ Không tải được ảnh cho {cat['id']}")
        except Exception as e:
            print(f"❌ Lỗi khi tải ảnh {cat['id']}: {e}")

# --- Main ---
if __name__ == "__main__":
    with open("cat_breeds_cleaned.json", "r", encoding="utf-8") as f:
        cat_data = json.load(f)

    try:
        conn = connect_db()
        cur = conn.cursor()

        create_table(cur)
        save_to_postgres(cur, cat_data)
        conn.commit()

        print("✅ Đã lưu metadata vào PostgreSQL.")

        download_images(cat_data)

    except Exception as e:
        print("❌ Lỗi:", e)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()