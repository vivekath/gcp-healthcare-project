import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit
from datetime import datetime
from common_lib.config_utils import load_schema_from_yaml

# Constants
CLIENT_ID = '09bf0f21-9dc3-41e0-966e-8ae3d476cc42_17a6ae0c-9a45-422d-a28b-796746818192'
CLIENT_SECRET = 'LygaivVEeV6GFKSgXOePgC7fB2eAf0aIxR2pqgtsPAQ='
TOKEN_ENDPOINT = 'https://icdaccessmanagement.who.int/connect/token'
API_VERSION = 'v2'
ACCEPT_LANGUAGE = 'en'
ROOT_URL = 'https://id.who.int/icd/release/10/2019/A00-A09'

# Function to obtain OAuth2 token
def get_access_token():
    payload = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'scope': 'icdapi_access',
        'grant_type': 'client_credentials'
    }
    response = requests.post(TOKEN_ENDPOINT, data=payload, verify=False)
    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        raise Exception(f"Failed to obtain access token: {response.status_code} - {response.text}")

# Function to make API requests
def fetch_icd_codes(url, headers):
    response = requests.get(url, headers=headers, verify=True)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

# Recursive function to extract ICD codes
def extract_codes(url, headers):
    data = fetch_icd_codes(url, headers)
    codes = []
    if 'child' in data:
        for child_url in data['child']:
            codes.extend(extract_codes(child_url, headers))
    else:
        if 'code' in data and 'title' in data:
            codes.append({
                'icd_code': data['code'],
                'icd_code_type': 'ICD-10',
                'code_description': data['title']['@value'],
                'inserted_date': datetime.now().date(),
                'updated_date': datetime.now().date(),
                'is_current_flag': True
            })
    return codes

# Get access token
access_token = get_access_token()

# Set headers for API requests
headers = {
    'Authorization': f'Bearer {access_token}',
    'Accept': 'application/json',
    'Accept-Language': ACCEPT_LANGUAGE,
    'API-Version': API_VERSION
}

# Extract ICD codes
icd_codes = extract_codes(ROOT_URL, headers)

# Define schema

icd_codes_schema = load_schema_from_yaml("schema/icd_codes.yaml", "icd_codes")
spark = SparkSession.builder.appName("ICD_Codes_Extraction").getOrCreate()

# Create DataFrame
df = spark.createDataFrame(icd_codes, schema=schema)

# # Show DataFrame
# df.show()

# Save to Parquet
df.write.format("parquet").mode("append").save("gs://healthcare-bucket-22032025/landing/icd_codes/")
