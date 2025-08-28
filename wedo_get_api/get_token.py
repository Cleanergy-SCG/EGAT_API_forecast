import requests
import time
import os
import pyodbc
import json
from enum import Enum
from datetime import datetime, timedelta
import logging
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)


WEDO_BASE_ENDPOINT="https://nlp.wedolabs.net/scgdofcst/"
headers = {
    # "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzg0NzI1ODUzLCJpYXQiOjE3NDE1MjU4NTMsImp0aSI6IjAxNzM2MWViYjhhNDQxY2FhYTZiM2NiMTE4MzA1ZWE3IiwidXNlcl9pZCI6MTR9.7D_x66pyassRwq1yYkIv7P8C-6_V1-QNg_0_OduMkqw",  # Your token with "Bearer " prefix
    "Content-Type": "application/json"
}

class WeatherStatus(Enum):
    Productive = "P"
    Stop = "S"
    Error = "E"
    NotProvide = "NP"

conn_str = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=20.212.208.119;'
    r'DATABASE=PEA_meter_data;'
    r'UID=postsavp;'
    r'PWD=Clean@100923;'
)
key_mapping = {
    "global_horizontal_irradiation": "global_horizontal_irradiation",
    "pv_module_temperature": "temperature_pv_module",
    "ambient_temperature": "temperature",
    "wind_speed_at_ground": "wind_speed_ground",
    "wind_direction_at_hub_height": "wind_direction",
    "global_horizontal_irradiation_01_w_per_m2": "global_horizontal_irradiation",
    "global_horizontal_irradiation_02_w_per_m2": "global_horizontal_irradiation",
    "pv_module_temperature_01_c": "temperature_pv_module",
    "ambient_temperature_01_c": "temperature",
    "ambient_temperature_02_c": "temperature",
    "wind_speed_at_ground_01_m_per_s": "wind_speed_ground",
    "wind_direction_at_hub_height_01_degree": "wind_direction"
}




def make_weather_template(datetime:str,weathertype:str,point:str,status:WeatherStatus,value:float,plantcode:str):
    mapping_key = key_mapping.get(weathertype, weathertype)
    raw_data =  {
        "datetime": datetime,
        "weathertype": mapping_key,
        "point": point,
        "status": status.value,  # Use .value to store the string
        "value": value,
        "plantcode": plantcode
    }
    # logger.info(f"{raw_data},{weathertype},{mapping_key}")
    
    return raw_data
    

def get_upload_server_config(agg_ca):
    data_output = []
    data_generator_AF = []
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
        total_response_actual_gen = {"data":[]}
        
        total_response_actual_weather = {"data":[]}
        
        for row in cursor.fetchall():   
            template = dict(zip(columns, row))
            data_output.append(template)
            response_actual_gen = {
                "datetime": "",
                "value": 0.0,
                "plantcode": "SKK7-N",
                "status": "P",
                "activepercentage": 0
            }

            dt = datetime.strptime(template["Datetime"], "%d/%m/%Y %H:%M:%S")
            iso_format = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            try:
                if template["total_power_mw"] <= 0:
                    response_actual_gen["status"] = "NP"
                elif template["total_power_mw"] == None:
                    response_actual_gen["status"] = "E"
            except:
                pass

            #add actual gen data
            response_actual_gen["datetime"] = iso_format
            response_actual_gen["value"] = template["total_power_mw"]
            total_response_actual_gen["data"].append(response_actual_gen)          
            #add actual weather data
            total_response_actual_weather["data"].append(make_weather_template(iso_format,"global_horizontal_irradiation_01_w_per_m2","01",WeatherStatus.Productive,template["global_horizontal_irradiation_01_w_per_m2"],"SKK7-N"))
            total_response_actual_weather["data"].append(make_weather_template(iso_format,"global_horizontal_irradiation_02_w_per_m2","02",WeatherStatus.Productive,template["global_horizontal_irradiation_02_w_per_m2"],"SKK7-N"))
            total_response_actual_weather["data"].append(make_weather_template(iso_format,"ambient_temperature_01_c","01",WeatherStatus.Productive,template["ambient_temperature_01_c"],"SKK7-N"))
            total_response_actual_weather["data"].append(make_weather_template(iso_format,"ambient_temperature_02_c","02",WeatherStatus.Productive,template["ambient_temperature_02_c"],"SKK7-N"))
            total_response_actual_weather["data"].append(make_weather_template(iso_format,"pv_module_temperature_01_c","01",WeatherStatus.Productive,template["pv_module_temperature_01_c"],"SKK7-N"))
            total_response_actual_weather["data"].append(make_weather_template(iso_format,"wind_speed_at_ground_01_m_per_s","01",WeatherStatus.Productive,template["wind_speed_at_ground_01_m_per_s"],"SKK7-N"))
            total_response_actual_weather["data"].append(make_weather_template(iso_format,"wind_direction_at_hub_height_01_degree","01",WeatherStatus.Productive,template["wind_direction_at_hub_height_01_degree"],"SKK7-N"))
            
            # break
        


        sql = """WITH workingtime AS (
            SELECT
                DATEADD(MINUTE, DATEDIFF(MINUTE, 0, t0.datetime) / 15 * 15, 0) AS interval_time,
                t3.plantCode,
                COUNT(*) AS WorkingDay
            FROM
                [scgcehuawei].[dbo].[getDevRealKpi] t0
                INNER JOIN [scgcehuawei].[dbo].[getDevList] t2 ON t0.devId = t2.id 
                INNER JOIN [scgcehuawei].[dbo].[stations] t3 ON t2.stationCode COLLATE Thai_CI_AS = t3.plantCode COLLATE Thai_CI_AS
                INNER JOIN [EDMI].[dbo].[mapProjectCode] t4 ON t4.plantCode_huawei COLLATE Thai_CI_AS = t3.plantCode COLLATE Thai_CI_AS
            WHERE
                t0.devId != '1000000033963259'
                AND t0.datetime BETWEEN DATEADD(d,-1,GETDATE()) AND GETDATE()
                AND t3.plantCode = 'NE=34233551'
            GROUP BY
                DATEADD(MINUTE, DATEDIFF(MINUTE, 0, t0.datetime) / 15 * 15, 0),
                t3.plantCode
        ),
        downtime AS (
            SELECT
                DATEADD(MINUTE, DATEDIFF(MINUTE, 0, t0.datetime) / 15 * 15, 0) AS interval_time,
                t3.plantCode,
                COUNT(*) AS Downtime
            FROM
                [scgcehuawei].[dbo].[getDevRealKpi] t0
                INNER JOIN [scgcehuawei].[dbo].[getDevList] t2 ON t0.devId = t2.id
                INNER JOIN [scgcehuawei].[dbo].[stations] t3 ON t2.stationCode = t3.plantCode
                INNER JOIN [EDMI].[dbo].[mapProjectCode] t4 ON t4.plantCode_huawei COLLATE Thai_CI_AS = t3.plantCode COLLATE Thai_CI_AS
            WHERE
                t0.devId != '1000000033963259'
                AND inverter_state NOT IN (512, 513, 514)
                AND t0.datetime BETWEEN DATEADD(d,-1,GETDATE()) AND GETDATE()
                AND t3.plantCode = 'NE=34233551'
            GROUP BY
                DATEADD(MINUTE, DATEDIFF(MINUTE, 0, t0.datetime) / 15 * 15, 0),
                t3.plantCode
        )

        SELECT
            wt.interval_time AS [time],
            wt.plantCode,
            wt.WorkingDay,
            ISNULL(dt.Downtime, 0) AS Downtime,
            ROUND((CONVERT(FLOAT, wt.WorkingDay) - CONVERT(FLOAT, ISNULL(dt.Downtime, 0))) / CONVERT(FLOAT, wt.WorkingDay) * 100, 2) AS AF
        FROM
            workingtime wt
        LEFT JOIN
            downtime dt ON wt.interval_time = dt.interval_time AND wt.plantCode = dt.plantCode
        ORDER BY
            wt.interval_time;

        """
        cursor.execute(sql)
        columns = [column[0] for column in cursor.description]
        raw_activepercentage = []
        for row in cursor.fetchall():   
            template = dict(zip(columns, row))
            raw_activepercentage.append({
                "time": template['time'],  # your CTE outputs `interval_time` AS [time]
                "AF": template['AF']
            })

        # Make AF lookup dict
        af_lookup = {}
        for item in raw_activepercentage:
            # Example: convert SQL datetime to same format as iso_format
            dt = item["time"]
            if isinstance(dt, str):
                dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")  # adjust to actual format
            iso_format = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            af_lookup[iso_format] = item["AF"]

        for record in total_response_actual_gen["data"]:
            ts = record["datetime"]
            if ts in af_lookup:
                record["activepercentage"] = af_lookup[ts]
            else:
                record["activepercentage"] = 0  # or 0 or keep original
        
        # for index in range(0,len(total_response_actual_gen["data"])):
        #     total_response_actual_gen["data"][index]["activepercentage"] = raw_activepercentage[index]

        conn.commit()
        # logger.info(f"{total_response_actual_gen}")
        # logger.info(f"{total_response_actual_weather}")
    except pyodbc.Error as e:
        logger.error(f"Error:{e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()
    return total_response_actual_gen["data"],total_response_actual_weather["data"]

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

data = {
    "username":"user_cleanergy",
    "password":"9@f1x*IF*J}*J:x"
}
token = get_token_wedo(data)
# print(token['access_token'])

def forward_weather_data_to_WEDO(query_data,token):
    headers = {
        "Authorization": "Bearer "+str(token['access_token']),  # Your token with "Bearer " prefix
        "Content-Type": "application/json"
    }
    # print(headers)
    try:
        # response = requests.post(url, json=data, headers=headers)
        response = requests.post(f"{WEDO_BASE_ENDPOINT}actualweather", json=(query_data), headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        # logger.info(f"API response: {response.json()},{response}") # or response.text
        logger.info(f"[forward_weather_data_to_WEDO] API status: {response.status_code},")# body preview: {str(response.text)[:200]}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error forwarding query to API: {e}")
        if e.response is not None:
            logger.error(f"Status code: {e.response.status_code}")
            # logger.error(f"Response body: {e.response.text}")

def forward_gen_data_to_WEDO(query_data,token):
    headers = {
        "Authorization": "Bearer "+str(token['access_token']),  # Your token with "Bearer " prefix
        "Content-Type": "application/json"
    }
    try:
        # response = requests.post(url, json=data, headers=headers)
        response = requests.post(f"{WEDO_BASE_ENDPOINT}actualgen", json=(query_data), headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        # logger.info(f"API response: {response.json()},{response}") # or response.text
        logger.info(f"[forward_gen_data_to_WEDO] API status: {response.status_code},")# body preview: {str(response.text)[:200]}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error forwarding query to API: {e}")
        if e.response is not None:
            logger.error(f"Status code: {e.response.status_code}")
            logger.error(f"Response body: {e.response.text}")


if __name__ == "__main__":
    try:
        total_response_actual_gen,total_response_actuat_weather = get_upload_server_config("949999990006")
        logger.info("Start forwarding.")
        # print(total_response_actuat_weather)
        token = get_token_wedo(data)
        forward_weather_data_to_WEDO(total_response_actuat_weather,token)
        forward_gen_data_to_WEDO(total_response_actual_gen,token)
        logger.info("Job finished.")
        # logger.info("Scheduler started. Running every 15 minutes.")
    except Exception as e:
        logger.error(f"Job not pass{e}")