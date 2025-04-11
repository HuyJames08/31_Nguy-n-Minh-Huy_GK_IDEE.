import requests
import json

DEFAULT_IMAGE_URL = "https://cdn2.thecatapi.com/images/aae.jpg"

def crawl_cat():
    url = "https://api.thecatapi.com/v1/breeds"
    response = requests.get(url)

    if response.status_code == 200:
        cats = response.json()
        print(f"Có {len(cats)} giống mèo được crawl.")

        results = []
        for cat in cats:
            results.append({
                "id": cat.get("id"),
                "name": cat.get("name"),
                "origin": cat.get("origin"),
                "temperament": cat.get("temperament"),
                "life_span": cat.get("life_span"),
                "image_url": cat.get("image", {}).get("url", DEFAULT_IMAGE_URL) or DEFAULT_IMAGE_URL
            })

        # In thử 5 con đầu tiên
        for cat in results[:5]:
            print(cat)

        return results
    else:
        print(f"Error: {response.status_code}")
        return []

def clean_cat(cats: list):
    cleaned = []
    for cat in cats:
        # Làm sạch temperament
        temperament = cat.get("temperament")
        if temperament:
            temperament_list = [x.strip() for x in temperament.split(",")]
        else:
            temperament_list = []

        # Kiểm tra và gán image_url nếu thiếu
        image_url = cat.get("image_url") or DEFAULT_IMAGE_URL

        cleaned.append({
            "id": cat.get("id"),
            "name": cat.get("name"),
            "origin": cat.get("origin"),
            "temperament": temperament_list,
            "life_span": cat.get("life_span"),
            "image_url": image_url
        })
    return cleaned

if __name__ == "__main__":
    raw_data = crawl_cat()
    cleaned_data = clean_cat(raw_data)

    print("\n Dữ liệu sau khi làm sạch:")
    for cat in cleaned_data[:5]:
        print(cat)

    with open("cat_breeds_cleaned.json", "w", encoding="utf-8") as f:
        json.dump(cleaned_data, f, indent=4, ensure_ascii=False)

    print("\n✅ Đã lưu file 'cat_breeds_cleaned.json'")