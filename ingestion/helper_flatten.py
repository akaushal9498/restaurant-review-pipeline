import json
import os
import time
from tqdm import tqdm
import psutil  # For memory usage (optional, install with: pip install psutil)

# Config
INPUT_FILE = "/Users/anupkaushal/PycharmProjects/restaurant-review-pipeline/data/data.json"
OUTPUT_FLAT_RESTAURANTS = "data/flat_restaurants.json"
OUTPUT_FLAT_MENU = "data/flat_menu.json"

# Create output dir if it doesn't exist
os.makedirs("data", exist_ok=True)

restaurant_count = 0
menu_item_count = 0

start_time = time.time()

process = psutil.Process(os.getpid())

with open(INPUT_FILE, 'r') as infile:
    print(f"🔍 Loading data from {INPUT_FILE}...")
    raw_data = json.load(infile)

print("⚙️ Flattening data...")

with open(OUTPUT_FLAT_RESTAURANTS, 'w') as rest_out, open(OUTPUT_FLAT_MENU, 'w') as menu_out:
    for city, city_info in tqdm(raw_data.items(), desc="Processing cities"):
        city_link = city_info.get("link")
        restaurants = city_info.get("restaurants", {})

        for rest_id, rest_data in restaurants.items():
            # Flat restaurant-level record
            flat_rest = {
                "id": rest_id,
                "name": rest_data.get("name"),
                "city": city,
                "rating": rest_data.get("rating"),
                "rating_count": rest_data.get("rating_count"),
                "cost": rest_data.get("cost"),
                "cuisine": rest_data.get("cuisine"),
                "lic_no": rest_data.get("lic_no"),
                "link": city_link,
                "address": rest_data.get("address")
            }
            rest_out.write(json.dumps(flat_rest) + "\n")
            restaurant_count += 1

            # Optional: Flatten menu
            menu = rest_data.get("menu", {})
            for category, items in menu.items():
                for dish_name, details in items.items():
                    flat_dish = {
                        "restaurant_id": rest_id,
                        "restaurant_name": rest_data.get("name"),
                        "city": city,
                        "category": category,
                        "dish": dish_name,
                        "price": details.get("price"),
                        "veg_or_non_veg": details.get("veg_or_non_veg")
                    }
                    menu_out.write(json.dumps(flat_dish) + "\n")
                    menu_item_count += 1

end_time = time.time()
duration = end_time - start_time
memory_mb = process.memory_info().rss / (1024 * 1024)

print("\n✅ Flattening Complete!")
print(f"🍽 Restaurants: {restaurant_count}")
print(f"🥘 Menu Items  : {menu_item_count}")
print(f"⏱️ Duration    : {duration:.2f} seconds")
print(f"🧠 Peak Memory : {memory_mb:.2f} MB")
print(f"📄 Output Files: {OUTPUT_FLAT_RESTAURANTS}, {OUTPUT_FLAT_MENU}")
