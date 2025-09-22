import requests
import pyodbc
from datetime import datetime, timedelta
import time

def get_egat_forecast(plant, starttime, endtime, auth_token, cookie_value):
    url = f"https://faas.egat.co.th/api/getforecast/"
    params = {
        "plant": plant,
        "starttime": starttime,
        "endtime": endtime
    }
    headers = {
        "Authorization": auth_token
        # "Cookie": f"cookiesession1={cookie_value}"
    }
    
    response = requests.get(url, headers=headers, params=params,timeout=(5, 20))
    
    if response.status_code == 200:
        return response.json()  # Or response.text if it's not JSON
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

# Example usage:
plant = "SKK7-N"
# starttime = "2025-6-25 00:00"
# endtime = "2025-7-18 00:00"
starttime = datetime.strftime(datetime.now(), "%Y-%m-%d")
endtime = (datetime.now() + timedelta(days=3)).date()
# starttime = '2025-07-21'
# endtime = '2025-07-24'
auth_token = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzg0NzI1ODUzLCJpYXQiOjE3NDE1MjU4NTMsImp0aSI6IjAxNzM2MWViYjhhNDQxY2FhYTZiM2NiMTE4MzA1ZWE3IiwidXNlcl9pZCI6MTR9.7D_x66pyassRwq1yYkIv7P8C-6_V1-QNg_0_OduMkqw"  # Replace with actual token
cookie_value = "678B2BAAE552B9CE39BE1F8E306C89DD"
print( starttime, endtime)
forecast_data = get_egat_forecast(plant, starttime, endtime, auth_token, cookie_value)

# if forecast_data:
#     print(forecast_data)
# print(len(forecast_data['data']))
# for element in forecast_data['data']:
#     # print(element[])
#     if element['intraday'] != None:
#         print(element)

# exit()
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
model = '15'       # Refer to EGAT name

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

# --- egat_forecast: update + insert ---
update_sql_egat = """
UPDATE [dbo].[egat_forecast]
SET day_ahead = ?, intra_day = ?
WHERE timestamp = ?
"""

insert_sql_egat = """
INSERT INTO [dbo].[egat_forecast] (timestamp, day_ahead, intra_day)
SELECT ?, ?, ?
WHERE NOT EXISTS (
    SELECT 1 FROM [dbo].[egat_forecast] WHERE timestamp = ?
)
"""

batch_size = 100
for i, element in enumerate(forecast_data['data']):
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
    cursor_a.execute(update_sql_egat, (day_ahead_val, intra_day_val, time_val))
    cursor_a.execute(insert_sql_egat, (
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