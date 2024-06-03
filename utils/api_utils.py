import os

import requests
import json
from dotenv import load_dotenv

def get_base_endpoint():

    # Load environment variables from .env file
    load_dotenv()

    # Access the environment variables
    shop_name = os.getenv('SHOPIFY_SHOP_NAME')
    api_protocol = os.getenv('SHOPIFY_API_PROTOCOL')
    api_version = os.getenv('SHOPIFY_API_VERSION')

    # Ensure the shop_name and access_token are loaded correctly
    if not shop_name or not api_protocol or not api_version:
        raise ValueError("Shop name or api protocol or api version not found in environment variables")

    # Construct the URL correctly
    return f'{api_protocol}://{shop_name}/admin/api/{api_version}/'

def get_header():

    access_token = os.getenv('SHOPIFY_ADMIN_API_ACCESS_TOKEN')

    # Ensure the shop_name and access_token are loaded correctly
    if not access_token:
        raise ValueError("Access token not found in environment variables")

    # Set up headers
    return {
        'Content-Type': 'application/json',
        'X-Shopify-Access-Token': access_token
    }

def get_api_url(final_endpoint):

    return f'{get_base_endpoint()}{final_endpoint}'

def get_graphql_url():

    return f'{get_base_endpoint()}graphql.json'

def get_response(final_endpoint):

    url = get_api_url(final_endpoint)
    header = get_header()

    # Make the API request
    response = requests.get(url = url, headers = header)

    # Handle response
    if response.status_code == 200:
        customers = response.json()
        print(customers)
    else:
        print(f"Failed to fetch data: {response.status_code}, {response.text}")

def execute_graphql_query(query):

    url = get_graphql_url()
    header = get_header()

    response = requests.post(url, json={'query': query}, headers=header)

    if response.status_code == 200:
        print("Query Result:")
        print(json.dumps(response.json(), indent=4))
    else:
        print(f"Query failed with status code {response.status_code}")
        print(response.text)