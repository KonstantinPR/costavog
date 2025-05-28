from app import app
import requests
from datetime import datetime


def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{app.config['TELEGRAM_BOT_TOKEN']}/sendMessage"
    params = {
        'chat_id': app.config['TELEGRAM_CHAT_ID'],
        'text': message
    }
    try:
        response = requests.get(url, params=params)
        print(f"Telegram response: {response.text}")
    except Exception as e:
        print(f"Error sending Telegram message: {e}")


def get_wildberries_acceptance_coefficients(selected_stores, api_key):
    url = "https://supplies-api.wildberries.ru/api/v1/acceptance/coefficients"
    headers = {
        'Authorization': api_key
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        json_data = response.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []

    exclude_warehouses = ["Алматы Атакент", "Астана 2", "СЦ Ереван"]
    filtered_stores = []

    for item in json_data:
        coefficient = item.get("coefficient", 0)
        warehouse_name = item.get("warehouseName", "")
        box_type_name = item.get("boxTypeName", "")

        if coefficient > 0 and box_type_name == "Короба":
            if warehouse_name in exclude_warehouses:
                continue
            if warehouse_name not in selected_stores:
                continue
            filtered_stores.append({
                "date": item.get("date", ""),
                "coefficient": coefficient,
                "warehouseID": item.get("warehouseID", ""),
                "warehouseName": warehouse_name,
                "allowUnload": item.get("allowUnload", ""),
                "boxTypeName": box_type_name,
                "deliveryCoef": item.get("deliveryCoef", ""),
                "storageCoef": item.get("storageCoef", "")
            })

    return filtered_stores


def process_and_notify(stores_data):
    # Fetch data

    all_stores_msg = ""
    for store in stores_data:
        store_date = store["date"][:10] if store["date"] else ""
        store_name = store["warehouseName"]
        store_coefficient = store["coefficient"]
        print(f"{store_name} | {store_date} | {store_coefficient}")
        all_stores_msg += f"{store_name} | {store_date} | {store_coefficient} | "

    print(f"Process completed at {datetime.now()}")
    return all_stores_msg
