import logging

import pandas as pd
from app import app
from app.modules import io_output, request_handler, yandex_disk_handler, API_WB, pandas_handler
import requests
from random import randint


def batched_get_rating(col_name='Артикул', testing_mode=True, is_update=True, batch_size=500):
    # Retrieve unique nmIDs from the API
    nmIDs = API_WB.get_all_cards_api_wb(testing_mode=testing_mode)['nmID'].unique()

    print(f"nmIDs is {len(nmIDs)} cards ...")

    # Split nmIDs into batches
    nmID_batches = [nmIDs[i:i + batch_size] for i in range(0, len(nmIDs), batch_size)]

    # Iterate through batches
    gotten_cards = batch_size
    current_df = pd.DataFrame
    for nmID_batch in nmID_batches:
        print(f"getting ratings on {gotten_cards} cards ...")

        # Create a new DataFrame to store ratings
        rating_df = pd.DataFrame(columns=[col_name, 'Rating', 'Feedbacks'])

        # Fill in the 'Артикул' column with the current batch of nmIDs
        rating_df[col_name] = nmID_batch

        # Download the current DataFrame from Yandex Disk
        current_df, _ = yandex_disk_handler.download_from_YandexDisk(path='RATING')

        # Update ratings in the current DataFrame
        rating_df = get_rating(rating_df, col_name=col_name, is_to_yadisk=False)

        # Set the 'key' column as index
        current_df.set_index(col_name, inplace=True)
        rating_df.set_index(col_name, inplace=True)

        # Update values in df1 using values from df2
        current_df.update(rating_df)

        # Reset index to make 'key' a column again
        current_df.reset_index(inplace=True)

        # Save the updated DataFrame to Yandex Disk
        io_df = io_output.io_output(current_df)
        file_name = f'rating.xlsx'
        logging.warning(f'Updated DataFrame uploaded to YandexDisk with name {file_name}')
        yandex_disk_handler.upload_to_YandexDisk(io_df, file_name=file_name, path=app.config['RATING'])
        gotten_cards += len(nmID_batch)

    return current_df


def get_rating(df, col_name='Артикул', is_to_yadisk=True):
    """Extract rating and number of feedbacks for each product ID."""
    print(f"get_rating ...")
    for index, row in df.iterrows():
        good_id = row[col_name]
        print(f"Getting: {good_id} ...")
        try:
            headers = {
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'ru,en;q=0.9',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) YaBrowser/22.9.4.866 Yowser/2.5 Safari/537.36'
            }
            url = f'https://card.wb.ru/cards/detail?spp=26&curr=rub&nm={good_id}'
            r = requests.get(url=url, headers=headers)
            if r.status_code == 200:
                logging.warning(f"Got it: {good_id}")
                data = r.json().get('data')
                if data and data.get('products'):
                    product = data['products'][0]
                    df.at[index, 'Rating'] = product.get('rating', '')
                    df.at[index, 'Feedbacks'] = product.get('feedbacks', '')
                    logging.warning(f"Rating: {df.at[index, 'Rating']}, Feedbacks {df.at[index, 'Feedbacks']}")
                else:
                    logging.warning(f"No product data found for ID: {good_id}")
            else:
                logging.error(f"Failed to retrieve data for ID: {good_id}. Status code: {r.status_code}")
        except Exception as e:
            logging.exception(f"An error occurred while processing ID: {good_id}. Error: {e}")

    return df
