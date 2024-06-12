from utils import api_utils
from utils import datetime_utils
from datetime import datetime, timedelta
import requests
import json

# Define the function to fetch orders with date range
def fetch_orders(start_date, end_date, cursor=None):
    query = f'''
    query getOrders($first: Int, $after: String) {{
      orders(first: $first, after: $after, query: "processed_at:>={datetime_utils.local_to_utc_date(start_date)}T00:00:00Z AND processed_at:<={datetime_utils.local_to_utc_date(end_date)}T23:59:59Z") {{
        pageInfo {{
          hasNextPage
          endCursor
        }}
        edges {{
          node {{
            id
            name
            createdAt
            processedAt
            updatedAt
            displayFinancialStatus
            customer {{
              id
            }}
            totalPriceSet {{
              shopMoney {{
                amount
                currencyCode
              }}
            }}
          }}
        }}
      }}
    }}
    '''
    
    variables = {
        "first": 10,  # Number of orders to fetch per request
        "after": cursor  # Pagination cursor
    }

    url = api_utils.get_graphql_url()
    header = api_utils.get_header()

    response = requests.post(url, json={'query': query, 'variables': variables}, headers=header)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Query failed with status code {response.status_code}: {response.text}")


# Function to collect order data into a dictionary based on date range
def collect_order_data(start_date, end_date):
    all_orders = []
    cursor = None

    while True:
        result = fetch_orders(start_date, end_date, cursor)
        
        if 'data' not in result or 'orders' not in result['data']:
            print("Unexpected response format:", json.dumps(result, indent=4))
            raise Exception("Unexpected response format.")
        
        orders = result['data']['orders']['edges']
        all_orders.extend(orders)
        
        page_info = result['data']['orders']['pageInfo']
        if not page_info['hasNextPage']:
            break
        
        cursor = page_info['endCursor']

    orders_dict = []
    for order in all_orders:
        order_data = order['node']
        order_id = order_data['id'].split("/")[-1]
        customer_id = order_data['customer']['id'].split("/")[-1]
        total_price = f"{order_data['totalPriceSet']['shopMoney']['amount']}"
        order_info = {
            "order_id": order_id,
            "order_name": order_data['name'],
            "created_at": order_data['createdAt'],
            "processed_at": order_data['processedAt'],
            "updated_at": order_data['updatedAt'],
            "financial_status": order_data['displayFinancialStatus'],
            "customer_id": customer_id,
            "total_price": total_price
        }
        orders_dict.append(order_info)

    return orders_dict


