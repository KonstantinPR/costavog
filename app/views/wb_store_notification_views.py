from app import app
from flask_login import login_required
from flask import jsonify
from app.modules.wb_telegram_store_notifications_module import process_and_notify, send_telegram_message, \
    get_wildberries_acceptance_coefficients


@app.route('/wb_store_notification_19734628/<region>', methods=['GET', 'POST'])
def wb_store_notification_19734628(region):
    # Now 'region' will contain 'south', 'center', etc.
    # You can define different inputs based on the region

    api_key = app.config['WB_DELIVERING']

    if region == 'center':
        selected_stores_input = "Рязань (Тюшевское)  Подольск 4  Сабурово  Владимир"
    elif region == 'south':
        selected_stores_input = "Волгоград  Невинномысск  Краснодар"
    elif region == 'volga':
        selected_stores_input = "Самара (Новосемейкино)  Сарапул  Казань"
    else:
        # default or error handling
        selected_stores_input = "Рязань (Тюшевское)  Подольск 4  Сабурово  Владимир  Волгоград  Невинномысск  Краснодар  Екатеринбург - Перспективный 12  Котовск  Тула"

    selected_stores = selected_stores_input.split("  ")

    stores_data = get_wildberries_acceptance_coefficients(selected_stores=selected_stores, api_key=api_key)
    all_stores_msg = process_and_notify(stores_data=stores_data)

    # Send message if data exists
    if all_stores_msg:
        send_telegram_message(all_stores_msg)

    return jsonify({"status": "success", "region": region, "message": all_stores_msg})
