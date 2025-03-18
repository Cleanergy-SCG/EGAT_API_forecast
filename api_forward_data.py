import requests
import schedule
import time
import os
import pyodbc
import json

conn_str = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=172.29.23.180;'
    r'DATABASE=PEA_meter_data;'
    r'UID=postsavp;'
    r'PWD=Clean@100923;'
)

def get_upload_server_config(agg_ca):
    data_output = []
    cursor = ""
    try:
        # Connect to the SQL Server database
        conn = pyodbc.connect(conn_str)

        # Create a cursor object
        cursor = conn.cursor()

        sql = """SELECT 
                FORMAT(total_power.Datetime, 'dd/MM/yyyy HH:mm:ss') AS Datetime
                --total_power.Datetime
                , total_power_mw
                , global_horizontal_irradiation_01_w_per_m2
                , global_horizontal_irradiation_02_w_per_m2
                , ambient_temperature_01_c
                , ambient_temperature_02_c
                , pv_module_temperature_01_c
                --, pv_module_temperature_02_c
                , wind_speed_at_ground_01_m_per_s
                --, wind_speed_at_ground_02_m_per_s
                , wind_direction_at_hub_height_01_degree
                --, wind_direction_at_hub_height_02_degree
                FROM(
                    SELECT
                        [pfwhim].Date_M AS 'Datetime'
                        ,([pfwhim].QtyValue) * 4 * ([dv].CTM / [dv].CTD * [dv].VTM / [dv].VTD) / 1000 /1000 AS 'total_power_mw'
                        FROM [EDMI].[dbo].[tblProfileWhImp] pfwhim
                        INNER JOIN [EDMI].[dbo].[tblMeterPoints] mtp ON pfwhim.[MeterPointId] = mtp.[MeterPointId]
                        INNER JOIN [EDMI].[dbo].[tblDevices] dv ON mtp.[Code] = dv.[SerialNumber]
                        INNER JOIN [EDMI].[dbo].[tblsiteinfo] sif ON sif.[SiteId] = dv.[SiteId]
                        WHERE SerialNumber = 251980953
                        AND Date_M BETWEEN DATEADD(d,-1,GETDATE()) AND GETDATE()
                        --ORDER BY Datetime ASC
                ) AS total_power

                LEFT JOIN 
                (
                    SELECT
                        --MAX(FORMAT(DateTime, 'dd/MM/yyyy HH:mm:ss')) AS Datetime
                        MAX(DateTime) AS Datetime
                        --,SUBSTRING(convert(varchar,Datetime,20),15,2)
                        --,convert(varchar,Datetime,20)
                        , SUM(CASE WHEN devId = '1000000034241641' THEN [radiant_line] END) AS 'global_horizontal_irradiation_01_w_per_m2'
                        , SUM(CASE WHEN devId = '1000000051508962' THEN [radiant_line] END) AS 'global_horizontal_irradiation_02_w_per_m2'
                        , SUM(CASE WHEN devId = '1000000034241641' THEN [temperature] END) AS 'ambient_temperature_01_c'
                        , SUM(CASE WHEN devId = '1000000051508962' THEN [temperature] END) AS 'ambient_temperature_02_c'
                        , SUM(CASE WHEN devId = '1000000034241641' THEN [pv_temperature] END) AS 'pv_module_temperature_01_c'
                        --, SUM(CASE WHEN devId = '1000000051508962' THEN [pv_temperature] END) AS 'pv_module_temperature_02_c' -- ไม่ได้เก็บค่า
                        , SUM(CASE WHEN devId = '1000000034241641' THEN [wind_speed] END) AS 'wind_speed_at_ground_01_m_per_s' -- เปลี่ยนชื่อตรงจุดวัด
                        --, SUM(CASE WHEN devId = '1000000051508962' THEN [wind_speed] END) AS 'wind_speed_at_ground_02_m_per_s'  -- เปลี่ยนชื่อตรงจุดวัด / ไม่ได้เก็บค่า
                        , SUM(CASE WHEN devId = '1000000034241641' THEN [wind_direction] END) AS 'wind_direction_at_hub_height_01_degree' -- เปลี่ยนชื่อตรงจุดวัด
                        --, SUM(CASE WHEN devId = '1000000051508962' THEN [wind_direction] END) AS 'wind_direction_at_hub_height_02_degree'  -- เปลี่ยนชื่อตรงจุดวัด / ไม่ได้เก็บค่า
                    FROM [scgcehuawei].[dbo].[getDevRealKpiEM]  
                    WHERE [devId] IN ('1000000034241641', '1000000051508962') 
                        AND Datetime  BETWEEN DATEADD(d,-1,GETDATE()) AND GETDATE()
                        AND (SUBSTRING(CONVERT(VARCHAR,Datetime,20),15,2) = '00'
                            OR SUBSTRING(CONVERT(VARCHAR,Datetime,20),15,2) = '15'
                            OR SUBSTRING(CONVERT(VARCHAR,Datetime,20),15,2) = '30'
                            OR SUBSTRING(CONVERT(VARCHAR,Datetime,20),15,2) = '45')
                    GROUP BY Datetime
                    --ORDER BY Datetime
                ) AS weather ON total_power.Datetime = weather.Datetime
                ORDER BY total_power.Datetime ASC
        """
        cursor.execute(sql)
        columns = [column[0] for column in cursor.description]
        
        for row in cursor.fetchall():   
            template = dict(zip(columns, row))
            data_output.append(template)
            break

        # print(data_output)

        conn.commit()

        # print("Data inserted successfully!")

    except pyodbc.Error as e:
        print("Error:", e)

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()
    return data_output
# API server URL (replace with your actual API URL)
API_SERVER_URL = os.environ.get("API_SERVER_URL", "http://localhost:8000/egat_test")

def forward_query_to_api(query_data):
    try:
        # response = requests.post(url, json=data, headers=headers)

        response = requests.post("http://localhost:8000/egat_test", json=query_data)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        print("API response:", response.json()) # or response.text
    except requests.exceptions.RequestException as e:
        print(f"Error forwarding query to API: {e}")


# 949999990006
def job():
    print("Running job...")
    try:
        query_data = get_upload_server_config("949999990006")
        print("config_result")
        forward_query_to_api(query_data)
        print("Job finished.")
    except Exception as e:
        print("Job not pass",e)

# Schedule the job to run every 15 minutes
schedule.every(15).minutes.do(job)

if __name__ == "__main__":
    query_data = get_upload_server_config("949999990006")
    print(type(query_data))
    json_str = json.dumps(query_data)
    forward_query_to_api(json_str)
    print("config_result")
    print("Scheduler started. Running every 15 minutes.")
    while True:
        schedule.run_pending()
        time.sleep(1)