import os

import requests
from dotenv import load_dotenv
from utils import api_utils

# final_endpoint_customers = "customers.json"
# api_utils.get_response(final_endpoint_customers)

api_utils.create_many_customers(3)



