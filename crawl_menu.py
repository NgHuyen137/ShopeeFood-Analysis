import requests
import time
import csv
import pandas as pd
import json
import concurrent.futures
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from tqdm import tqdm
from tenacity import retry, wait_exponential, stop_after_attempt

output_path = '/home/phuonn/Shopee_food/UDPTDLTM_DATA/data/menu_data.csv'
input_path = '/home/phuonn/Shopee_food/UDPTDLTM_DATA/data/delivery_info_clean.csv'
delivery_df = pd.read_csv(input_path)
id_list = delivery_df[['id', 'restaurant_id']]

def init_session():
    """Tạo session với retry để dùng lại cho các request."""
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

@retry(wait=wait_exponential(multiplier=1, min=2, max=10), stop=stop_after_attempt(5))
def request_dish_detail(session, restaurant_id):
    """Tạo yêu cầu API với retry để lấy chi tiết món ăn."""
    response = session.get(
        "https://gappapi.deliverynow.vn/api/dish/get_delivery_dishes",
        params={"id_type": 2, "request_id": restaurant_id},
        headers={
            "X-Foody-Access-Token": "",  
            "X-Foody-Api-Version": "1",
            "X-Foody-App-Type": "1004",
            "X-Foody-Client-Id": "",  
            "X-Foody-Client-Language": "vi",
            "X-Foody-Client-Type": "1",
            "X-Foody-Client-Version": "3.0.0"
        },
        timeout=(3, 10)  # Thời gian chờ kết nối là 3s và chờ phản hồi là 10s
    )
    response.raise_for_status()
    return response

# def get_dish_details(session, restaurant_id):
#     """Gửi yêu cầu API và lấy thông tin món ăn cho một restaurant_id."""
#     menu = []
#     response = request_dish_detail(session, restaurant_id)
#     json_data = response.json()

#     # Duyệt qua từng món ăn để lấy name và discount_price/price
#     for catalog in json_data.get("data", {}).get("catalogs", []):
#         for dish in catalog.get("dishes", []):
#             name = dish.get("name")
#             price = dish.get("discount_price", dish.get("price"))
#             if name and price:
#                 menu.append({"name": name, "price": int(price)})

#     return {'restaurant_id': restaurant_id, 'menu': menu}
def get_dish_details(session, request_id, restaurant_id):
    """Gửi yêu cầu API và lấy thông tin món ăn cho một restaurant_id."""
    dishes_list = []
    response = request_dish_detail(session, request_id)
    data = response.json()

    # Duyệt qua từng món ăn để lấy name và discount_price/price
    menu_infos = data.get('reply', {}).get('menu_infos', [])
    for menu in menu_infos:
        dishes = menu.get('dishes', [])

        for dish in dishes:
            name = dish.get('name', '')
            price = dish.get('price', {}).get('value', '')
            if name and price:  
                dishes_list.append({'name': name, 'price': int(price)})

    return {'restaurant_id': restaurant_id, 'menu': dishes_list}

def write_to_csv(output_csv, data):
    """Ghi dữ liệu vào file CSV với định dạng JSON dễ đọc."""
    records = [
        {"restaurant_id": item['restaurant_id'], "menu": json.dumps(item['menu'], ensure_ascii=False)}
        for item in data
    ]
    df = pd.DataFrame(records)
    df.to_csv(output_csv, mode='a', header=False, index=False, quoting=csv.QUOTE_NONNUMERIC)

def initialize_csv(output_csv=output_path):
    """Khởi tạo file CSV với header."""
    with open(output_csv, 'w') as f:
        f.write("restaurant_id,menu\n")

def get_dishes(id_list, output_csv=output_path, batch_size=200):
    """Lấy dữ liệu món ăn cho danh sách restaurant_id theo từng batch."""
    initialize_csv(output_csv)
    results = []
    session = init_session()
    total_len = len(id_list)

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        for start in tqdm(range(0, total_len, batch_size), desc="Processing batches"):
            batch_ids = id_list[start:start + batch_size]
            futures = {
                executor.submit(get_dish_details, session, int(row['id']), int(row['restaurant_id'])): (int(row['id']), int(row['restaurant_id']))
                for _, row in batch_ids.iterrows()
            }

            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as exc:
                    # print(f"Restaurant generated an exception: {exc}")
                    pass  

            if results:
                write_to_csv(output_csv, results)
                results = []

if __name__ == "__main__":
    get_dishes(id_list)
