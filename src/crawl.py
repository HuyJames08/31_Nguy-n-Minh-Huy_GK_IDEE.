import requests

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

if __name__ == "__main__":
    crawl_cat()