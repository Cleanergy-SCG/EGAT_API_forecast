import requests
import pyodbc
from datetime import datetime, timedelta
import time
import logging
import json
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

WEDO_BASE_ENDPOINT="https://nlp.wedolabs.net/scgdofcst/"

def get_token_wedo(query_data):
    try:
        
        response = requests.post(f"{WEDO_BASE_ENDPOINT}token", data=query_data)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        # logger.info(f"API response: {response.json()},{response}") # or response.text
        logger.info(f"API status: {response.status_code}, body preview: {str(response.text)[:200]}")
        # print(response.text)
        result = json.loads(response.text)
        # print()
        return result

    except requests.exceptions.RequestException as e:
        logger.error(f"Error forwarding query to API: {e}")
        if e.response is not None:
            logger.error(f"Status code: {e.response.status_code}")
            logger.error(f"Response body: {e.response.text}")



def get_egat_forecast(plant, starttime, endtime, auth_token, cookie_value):

    data = {
        "username":"user_cleanergy",
        "password":"9@f1x*IF*J}*J:x"
    }
    token = get_token_wedo(data)

    url = f"https://nlp.wedolabs.net/scgdofcst/getforecast"
    params = {
        "plant": plant,
        "starttime": starttime,
        "endtime": endtime
    }
    headers = {
        "Authorization": "Bearer "+str(token['access_token']),  # Your token with "Bearer " prefix
        "Content-Type": "application/json"
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()  # Or response.text if it's not JSON
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

# Example usage:
plant = "SKK7-N"
# starttime = "2025-6-25 00:00"
# endtime = "2025-7-18 00:00"
starttime = datetime.now().strftime("%Y-%m-%d 00:00")
endtime = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d 00:00")
# starttime = '2025-07-21'
# endtime = '2025-07-24'
# starttime = "2025-09-01 00:00"
# endtime = "2025-09-05 00:00"
auth_token = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzg0NzI1ODUzLCJpYXQiOjE3NDE1MjU4NTMsImp0aSI6IjAxNzM2MWViYjhhNDQxY2FhYTZiM2NiMTE4MzA1ZWE3IiwidXNlcl9pZCI6MTR9.7D_x66pyassRwq1yYkIv7P8C-6_V1-QNg_0_OduMkqw"  # Replace with actual token
cookie_value = "678B2BAAE552B9CE39BE1F8E306C89DD"
print( starttime, endtime)
forecast_data = get_egat_forecast(plant, starttime, endtime, auth_token, cookie_value)
if forecast_data['success'] == True:
    forecast_data = (forecast_data['response']['data'])
else:
    print("error no response")
    exit()

print(forecast_data)
# exit()
# if forecast_data:
#     print(forecast_data)
# print(len(forecast_data['data']))
# for element in forecast_data['data']:
#     # print(element[])
#     if element['intraday'] != None:
#         print(element)
print(forecast_data)
conn_a = pyodbc.connect(
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=scgcleanergyapi.database.windows.net;'
    r'DATABASE=PEA_meter_data;'
    r'UID=scgce;'
    r'PWD=Clean@100923;'
)

cursor_a = conn_a.cursor()
# Your fixed site and model name
site = '1'         # Replace with your site name
model = '18'       # Refer to EGAT name

print("start insert")

# --- forecast_data: update + insert ---
update_sql_forecast = """
UPDATE [dbo].[forecast_data]
SET actual = ?, forecast = ?
WHERE time = ? AND site = ? AND model = ?
"""

insert_sql_forecast = """
INSERT INTO [dbo].[forecast_data] (time, site, model, actual, forecast)
SELECT ?, ?, ?, ?, ?
WHERE NOT EXISTS (
    SELECT 1 FROM [dbo].[forecast_data] WHERE time = ? AND site = ? AND model = ?
)
"""

# --- wedo_forecast: update + insert ---
update_sql_wedo = """
UPDATE [dbo].[wedo_forecast]
SET day_ahead = ?, intra_day = ?
WHERE timestamp = ?
"""

insert_sql_wedo = """
INSERT INTO [dbo].[wedo_forecast] (timestamp, day_ahead, intra_day)
SELECT ?, ?, ?
WHERE NOT EXISTS (
    SELECT 1 FROM [dbo].[wedo_forecast] WHERE timestamp = ?
)
"""

batch_size = 100
for i, element in enumerate(forecast_data):
    dt = datetime.strptime(element['time'], "%Y-%m-%dT%H:%M:%S%z")
    formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    time_val = formatted_time

    # Forecast values
    forecast_val = element.get('dayahead')
    actual_val = None  # Placeholder if actual not available

    # Insert into forecast_data
    cursor_a.execute(update_sql_forecast, (actual_val, forecast_val, time_val, site, model))
    cursor_a.execute(insert_sql_forecast, (
        time_val, site, model, actual_val, forecast_val,
        time_val, site, model
    ))

    # EGAT forecast values
    day_ahead_val = element.get('dayahead')
    intra_day_val = element.get('intraday')

    # Insert into egat_forecast
    cursor_a.execute(update_sql_wedo, (day_ahead_val, intra_day_val, time_val))
    cursor_a.execute(insert_sql_wedo, (
        time_val, day_ahead_val, intra_day_val, time_val
    ))

    # Optional batch commit
    if i % batch_size == 0:
        conn_a.commit()

    time.sleep(0.1)

# Final commit and close
conn_a.commit()
conn_a.close()
print("Done")