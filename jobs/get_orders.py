from utils import api_utils
from datetime import datetime, timedelta
import requests
import json

# Define the function to fetch orders with date range
def fetch_orders(start_date, end_date, cursor=None):
    
    cond = f"created_at:>={start_date} created_at:<={end_date}"
    
    if start_date == end_date:
        end_date_as_date = datetime.strptime(end_date, "%Y-%m-%d")
        end_date_as_date_plus_1 = end_date_as_date + timedelta(days=1)
        new_end_date_as_str = end_date_as_date_plus_1.strftime("%Y-%m-%d")
        cond = f"created_at:={start_date}"

    print(cond)

    query = f'''
    query getOrders($first: Int, $after: String) {{
      orders(first: $first, after: $after, query: "{cond}") {{
        pageInfo {{
          hasNextPage
          endCursor
        }}
        edges {{
          node {{
            id
            name
            createdAt
            displayFinancialStatus
            customer {{
              firstName
              lastName
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
        customer_name = f"{order_data['customer']['firstName']} {order_data['customer']['lastName']}"
        total_price = f"{order_data['totalPriceSet']['shopMoney']['amount']} {order_data['totalPriceSet']['shopMoney']['currencyCode']}"
        order_info = {
            "Order ID": order_data['id'],
            "Order Name": order_data['name'],
            "Created At": order_data['createdAt'],
            "Financial Status": order_data['displayFinancialStatus'],
            "Customer Name": customer_name,
            "Total Price": total_price
        }
        orders_dict.append(order_info)

    return orders_dict

# Example usage: Collect orders data within the date range
start_date = "2024-05-30"
end_date = "2024-05-30"
orders_data = collect_order_data(start_date, end_date)

# Print all fetched orders from the dictionary
for order in orders_data:
    print(f"Order ID: {order['Order ID']}")
    print(f"Order Name: {order['Order Name']}")
    print(f"Created At: {order['Created At']}")
    print(f"Financial Status: {order['Financial Status']}")
    print(f"Customer Name: {order['Customer Name']}")
    print(f"Total Price: {order['Total Price']}")
    print("="*40)
