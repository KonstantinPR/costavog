import requests
import pandas as pd
import json



# Function to split list into chunks of size <= 1000
def update_api_with_chank(prices_list_all, results_list, headers):

    def chunk_list(lst, chunk_size=1000):
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]

    # API details
    api_url = 'https://api-seller.ozon.ru/v1/product/import/prices'

    all_results_df = pd.DataFrame()

    for chunk in chunk_list(prices_list_all, 1000):
        payload = {"prices": chunk}
        json_payload = json.dumps(payload, ensure_ascii=False)

        try:
            response = requests.post(api_url, headers=headers, data=json_payload)
            print("Response status code:", response.status_code)
            print("Raw response JSON:", response.text)
            response.raise_for_status()

            resp_json = response.json()
            print("Parsed JSON response:", json.dumps(resp_json, indent=2))

            results_list_response = resp_json.get('result', [])
            if not results_list_response:
                print("No 'result' key or empty 'result' in response.")
            else:
                print(f"Received {len(results_list_response)} results from API.")

            # Map product_id to response data
            response_map = {item.get('product_id'): item for item in results_list_response}

            # Update results_list with actual response info
            for result in results_list:
                if result['product_id'] in response_map:
                    api_result = response_map[result['product_id']]
                    if api_result.get('errors'):
                        result['status'] = 'Failed'
                        error_messages = api_result['errors']
                        result['response_message'] = '; '.join([err.get('message', '') for err in error_messages])
                    elif api_result.get('updated'):
                        result['status'] = 'Success'
                        result['response_message'] = 'Price updated successfully'
                    else:
                        result['status'] = 'Failed'
                        result['response_message'] = 'Unknown response'
                else:
                    # No response for this product
                    result['status'] = 'Failed'
                    result['response_message'] = 'No response for this product'

            # Append current batch results
            all_results_df = pd.concat([all_results_df, pd.DataFrame(results_list)], ignore_index=True)

        except requests.exceptions.RequestException as e:
            print("Error during API request:", e)
            # Mark all in current batch as failed
            for result in results_list:
                result['status'] = 'Failed'
                result['response_message'] = str(e)
            all_results_df = pd.concat([all_results_df, pd.DataFrame(results_list)], ignore_index=True)
    return all_results_df
