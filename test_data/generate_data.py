from utils import api_utils
from faker import Faker
import random
import requests
import time
import datetime
import pytz

def random_datetime(start, end):
    """
    Generate a random datetime between `start` and `end`.
    """
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + datetime.timedelta(seconds=random_seconds)

def random_datetime_in_five_years():

    # Define the current datetime with timezone
    end_datetime = datetime.datetime.now(pytz.timezone('America/Chicago')) - datetime.timedelta(7)

    # Define the start datetime as 5 years before the current datetime
    start_datetime = end_datetime - datetime.timedelta(days=1*365)

    # Generate a random datetime between start_datetime and end_datetime
    random_generated_datetime = random_datetime(start_datetime, end_datetime)

    # Format the random datetime in the specified format
    return {
        "deposit_created_at" : random_generated_datetime.strftime('%Y-%m-%dT%H:%M:%S%z'),
        "remaining_created_at" : (random_generated_datetime + datetime.timedelta(1)).strftime('%Y-%m-%dT%H:%M:%S%z'),
        "deposit_paid_datetime" : (random_generated_datetime + datetime.timedelta(3)).strftime('%Y-%m-%dT%H:%M:%S%z'),
        "remaining_paid_datetime" : (random_generated_datetime + datetime.timedelta(5)).strftime('%Y-%m-%dT%H:%M:%S%z'),
        "pickup_datetime" : (random_generated_datetime + datetime.timedelta(6)).strftime('%Y-%m-%dT%H:%M:%S%z'),
    }

def random_line_items(order_type):

    line_items_options = [{
        "title":f"6 inch base cake {order_type}",
        "price":100.00,
        "grams":"1300",
        "quantity":1,
        "tax_lines":[{
            "price":13.5,
            "rate":0.06,
            "title":"State tax"
            }]
        },{
        "title":f"8 inch base cake {order_type}",
        "price":150.00,
        "grams":"1300",
        "quantity":1,
        "tax_lines":[{
            "price":13.5,
            "rate":0.06,
            "title":"State tax"
            }]
        },{
        "title":f"10 inch base cake {order_type}",
        "price":200.00,
        "grams":"1300",
        "quantity":1,
        "tax_lines":[{
            "price":13.5,
            "rate":0.06,
            "title":"State tax"
            }]
        }]
    
    random_index = random.randint(0,2)
    line_items = [line_items_options[random_index]]
    
    decoration = {
        "title":f"Decoration",
        "price":20.00,
        "grams":"10",
        "quantity":1
    }

    if order_type == 'remaining':
        line_items.append(decoration)

    return line_items

def create_single_customer():

    final_endpoint_customers = "customers.json"
    url = api_utils.get_api_url(final_endpoint_customers)
    header = api_utils.get_header()

    fake = Faker()
    full_name = fake.name().split(" ")
    f_name = full_name[0]
    l_name = full_name[1]
    email = f'{f_name}.{l_name}@example.com'
    phone = f'+1512{random.randint(100, 999)}{random.randint(1000, 9999)}'
    full_address = fake.address().split("\n")
    address1 = full_address[0]
    addresses = [{
        "address1":address1,
        "city":"Austin",
        "province":"TX",
        "phone":phone,
        "zip":"12345",
        "last_name":l_name,
        "first_name":f_name,
        "country":"USA"
    }]

    customer_data = {
    "customer": {
        "first_name": f_name,
        "last_name": l_name,
        "email": email,
        "phone": phone,
        "addresses": addresses
    }}

    print(customer_data)

    response = requests.post(url, headers=header, json=customer_data)
    print(response.json())
    if response.status_code == 201:
        print(f"Customer {f_name} {l_name} created.")
    else:
        print(f"Cannot Create the customer with adress {full_address}")
        print(f"Failed to fetch data: {response.status_code}, {response.text}")

def create_many_customers(count):

    for i in range(count):

        time.sleep(0.5)

        create_single_customer()

def get_customer_ids():

    # We can only bring maximum 250 customers
    final_endpoint_customers = "customers.json?limit=250"
    url = api_utils.get_api_url(final_endpoint_customers)
    header = api_utils.get_header()

    # Fetch customers from the API
    response = requests.get(url, headers=header)

    # Check if the request was successful
    if response.status_code == 200:
        customers = response.json().get('customers', [])
        customer_ids = [customer['id'] for customer in customers]
        # print(len(customer_ids))
        # print("Customer IDs:", customer_ids)
    else:
        print("Failed to fetch customers:", response.status_code, response.text)

    return customer_ids

def get_order_ids():

    # We can only bring maximum 250 customers
    final_endpoint_customers = "orders.json?limit=250"
    url = api_utils.get_api_url(final_endpoint_customers)
    header = api_utils.get_header()

    # Fetch customers from the API
    response = requests.get(url, headers=header)

    # Check if the request was successful
    if response.status_code == 200:
        orders = response.json().get('orders', [])
        order_ids = [order['id'] for order in orders]
        # print(len(customer_ids))
        # print("Customer IDs:", customer_ids)
    else:
        print("Failed to fetch orders:", response.status_code, response.text)

    return order_ids

def create_metafields_for_order(
        order_id,
        order_type,
        pickup_datetime,
        rand_indexes # expecting length 3 array with random values from 0,1,2
):
    
    final_endpoint_customers = f"orders/{order_id}/metafields.json"
    url = api_utils.get_api_url(final_endpoint_customers)
    header = api_utils.get_header()

    flavor = ["chocolate", "vanilla", "red velvet"]
    theme = ["star wars", "barbie", "lord of the rings"]
    allergies = ["n/a","dairy","peanut"]

    metafields_data = [
    {
        "namespace": "custom",
        "key": "draft_type",
        "type": "single_line_text_field",
        "value": order_type
    },
    {
        "namespace": "custom",
        "key": "theme",
        "type": "single_line_text_field",
        "value": theme[rand_indexes[0]]
    },
    {
        "namespace": "custom",
        "key": "flavor",
        "type": "single_line_text_field",
        "value": flavor[rand_indexes[1]]
    },
    {
        "namespace": "custom",
        "key": "allergies",
        "type": "single_line_text_field",
        "value": allergies[rand_indexes[2]]
    },
    {
        "namespace": "custom",
        "key": "pickup_date",
        "type": "date_time",
        "value": pickup_datetime
    }
]


    for metafield in metafields_data:

        print(metafield["key"])

        response = requests.post(url, headers=header, json={"metafield": metafield})
        if response.status_code == 201:
            # print(response.json())
            print(f"Metafield {metafield} are created for {order_id}.")
        else:
            print(f"Cannot Create the metafield")
            print(f"Failed to fetch data: {response.status_code}, {response.text}")

        time.sleep(1)

def get_metafields_by_order(order_id):

    final_endpoint_customers = f"orders/{order_id}/metafields.json"
    url = api_utils.get_api_url(final_endpoint_customers)
    header = api_utils.get_header()

    response = requests.get(url, headers=header)
    if response.status_code == 200:
        metafields = response.json()
        ids = [m["id"] for m in metafields["metafields"]]
        print(metafields)
        print(f"Order with id {ids} is retrieved.")
    else:
        print(f"Failed to fetch data: {response.status_code}, {response.text}")


def create_order_by_customer(
        customer_id,
        financial_status,
        order_type,
        created_at = datetime.datetime.now(pytz.timezone('America/Chicago')).strftime('%Y-%m-%dT%H:%M:%S%z')
    ):

    final_endpoint_customers = "orders.json"
    url = api_utils.get_api_url(final_endpoint_customers)
    header = api_utils.get_header()

    line_items = random_line_items(order_type)

    order_data = {
        "order":{
            "created_at":created_at,
            "tags":order_type,
            "line_items":line_items,
            "customer":{"id":customer_id},
            "financial_status":financial_status}
        }
    
    response = requests.post(url, headers=header, json=order_data)
    if response.status_code == 201:
        order = response.json()
        order_id = order["order"]["id"]
        order_number = order["order"]["order_number"]
        print(f"Order with id {order_id} and number {order_number} created.")
    else:
        print(f"Cannot Create the order")
        print(f"Failed to fetch data: {response.status_code}, {response.text}")
        return Exception("Failed to create order")

    return order_id

def create_paid_orders_for_random_customers(count):

    customer_ids = sorted(get_customer_ids())
    random_elements = [random.choice(customer_ids) for _ in range(count)]
    created_at_datetimes = random_datetime_in_five_years()

    for i in random_elements:

        rand_indexes = [random.randint(0, 2) for _ in range(3)]

        time.sleep(3)

        deposit_order_id = create_order_by_customer(
            customer_id = i,
            financial_status = "paid",
            order_type = "deposit",
            created_at=created_at_datetimes["deposit_created_at"]
        )

        create_metafields_for_order(
            order_id=deposit_order_id,
            order_type="deposit",
            pickup_datetime=created_at_datetimes["pickup_datetime"],
            rand_indexes=rand_indexes
        )

        time.sleep(0.5)

        remaining_order_id = create_order_by_customer(
            customer_id = i,
            financial_status = "paid",
            order_type = "remaining",
            created_at=created_at_datetimes["remaining_created_at"]
        )

        create_metafields_for_order(
            order_id=remaining_order_id,
            order_type="remaining",
            pickup_datetime=created_at_datetimes["pickup_datetime"],
            rand_indexes=rand_indexes
        )

def delete_order(order_id):

    final_endpoint_customers = f"orders/{order_id}.json"
    url = api_utils.get_api_url(final_endpoint_customers)
    header = api_utils.get_header()

    response = requests.delete(url, headers=header)
    if response.status_code == 200:
        print(f"{order_id} deleted.")
    else:
        print(f"Cannot delete order")
        print(f"Failed to fetch data: {response.status_code}, {response.text}")

def delete_all_orders():

    order_ids = get_order_ids()

    for order_id in order_ids:

        time.sleep(0.5)

        delete_order(order_id)
    

# delete_all_orders()    

# create_many_customers(10)
# create_single_customer()
create_paid_orders_for_random_customers(1000)

# create_metafields_for_order(
#     5551028961418,
#     "deposit",
#     random_datetime_in_five_years()["pickup_datetime"],
#     [0,1,2]
# )

# get_metafields_by_order(5551017885834)
