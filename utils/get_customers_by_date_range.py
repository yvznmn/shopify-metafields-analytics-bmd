from utils import api_utils
from utils import datetime_utils
from datetime import datetime, timedelta
import requests
import json

# Define the function to fetch customers with date range
def fetch_customers(start_date, end_date, cursor=None):
    query = f'''
    query getCustomers($first: Int, $after: String) {{
      customers(first: $first, after: $after, query: "updated_at:>={datetime_utils.local_to_utc_date(start_date)}T00:00:00Z AND updated_at:<={datetime_utils.local_to_utc_date(end_date)}T23:59:59Z") {{
        pageInfo {{
          hasNextPage
          endCursor
        }}
        edges {{
          node {{
            id
            firstName
            lastName
            email
            phone
            numberOfOrders
            updatedAt
          }}
        }}
      }}
    }}
    '''
    
    variables = {
        "first": 10,  # Number of customers to fetch per request
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
def collect_customer_data(start_date, end_date):
    all_customers = []
    cursor = None

    while True:
        result = fetch_customers(start_date, end_date, cursor)
        
        if 'data' not in result or 'customers' not in result['data']:
            print("Unexpected response format:", json.dumps(result, indent=4))
            raise Exception("Unexpected response format.")
        
        customers = result['data']['customers']['edges']
        all_customers.extend(customers)
        
        page_info = result['data']['customers']['pageInfo']
        if not page_info['hasNextPage']:
            break
        
        cursor = page_info['endCursor']

    customers_dict = []
    for customer in all_customers:
        customer_data = customer['node']
        customer_id = customer_data['id'].split("/")[-1]
        customer_info = {
            "customer_id": customer_id,
            "first_name": customer_data['firstName'],
            "last_name": customer_data['lastName'],
            "email": customer_data['email'],
            "phone": customer_data['phone'],
            "number_of_orders": customer_data['numberOfOrders'],
            "updated_at": customer_data['updatedAt']
        }
        customers_dict.append(customer_info)

    return customers_dict

# # Example usage
# start_date = '2024-06-14'
# end_date = '2024-06-17'
# customer_data = collect_customer_data(start_date, end_date)
# print(json.dumps(customer_data, indent=4))

