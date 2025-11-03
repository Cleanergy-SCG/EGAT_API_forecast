import requests
import schedule
import time
import os
import pyodbc
import json
from enum import Enum
from datetime import datetime, timedelta
import pandas as pd
import logging
import math
import numpy as np
import warnings
warnings.simplefilter(action="ignore", category=UserWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)



EGAT_BASE_URL = "https://faas.egat.co.th/api/qas"

class WeatherStatus(Enum):
    Productive = "P"
    Stop = "S"
    Error = "E"
    NotProvide = "NP"

headers = {
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxODA1MzM1NjA1LCJpYXQiOjE3NjIxMzU2MDUsImp0aSI6IjI2MTBjNDNhMTJlNTRmNjQ5N2NjNGMxNmJlMDU0ZGE4IiwidXNlcl9pZCI6MTd9.n6iFkEwwYRjCXDMdzjSuCKn1FkQaUu2cjGvFFSi5MUo",  # Your token with "Bearer " prefix
    "Content-Type": "application/json"
}


conn_str = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=20.212.208.119;'
    r'DATABASE=PEA_meter_data;'
    r'UID=postsavp;'
    r'PWD=Clean@100923;'
)

CONN_AZ_SQL = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=scgcleanergyapi.database.windows.net;'
    r'DATABASE=PEA_meter_data;'
    r'UID=scgce;'
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
    "wind_direction_at_hub_height_01_degree": "wind_direction",
    "wind_speed_m_per_s": "wind_speed_ground",
    "wind_direction_degree": "wind_direction"
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


# assumes: key_mapping, make_weather_template, get_sa_features, get_active_percentage are defined

def _to_iso_z(dt_like):
    """Return 'YYYY-MM-DDTHH:MM:SSZ' from datetime/str already at 15-min resolution."""
    if isinstance(dt_like, str):
        # be lenient with common formats
        try:
            dt = datetime.fromisoformat(dt_like.replace("Z",""))
        except ValueError:
            dt = datetime.strptime(dt_like, "%Y-%m-%d %H:%M:%S")
    else:
        dt = pd.to_datetime(dt_like).to_pydatetime()
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def _safe_float(x, default=0.0):
    if x is None:
        return default
    if isinstance(x, (float, int)):
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return default
        return float(x)
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:
        return default

def _status_and_value(raw):
    safe = _safe_float(raw, None)       # preserve None if invalid
    if safe is None:
        return WeatherStatus.NotProvide, 0.0
    return WeatherStatus.Productive, safe

def build_upload_payloads(conn_str: str, plant_code: str, start_dt, end_dt, plantcode_label: str = None):
    """
    Returns (total_response_actual_gen, total_response_actual_weather)
    compatible with your server uploader format, using dynamic plant_code/start/end.
    """
    if plantcode_label is None:
        plantcode_label = plant_code  # keep same label unless you want a friendly alias

    if plantcode_label == "TH-007-SRT_Solar Rooftop Saha Union":
        plantcode_label = "SHU-PV"
    elif plantcode_label == "TH-008-SRF_Solar Farm Saha Union":
        plantcode_label = "SHU-N"
    # 1) Pull features (power + weather averaged)
    df = get_sa_features(plant_code=plant_code, start_dt=start_dt, end_dt=end_dt)
    if df.empty:
        return {"data": []}, {"data": []}

    # enforce expected columns coming from get_sa_features()
    # columns: datetime, total_power_mw, global_horizontal_irradiation_w_per_m2, ambient_temperature_c, pv_module_temperature_c

    # 2) Pull AF and build lookup by the same 15-min ISO key
    af_rows = get_active_percentage(conn_huawei=conn_str, plant_code=plant_code, start_dt=start_dt, end_dt=end_dt)
    af_lookup = {}
    for item in af_rows:
        iso_key = _to_iso_z(item["time"])
        af_lookup[iso_key] = _safe_float(item.get("AF", 0.0), 0.0)

    # 3) Build payloads
    total_response_actual_gen = {"data": []}
    total_response_actual_weather = {"data": []}

    for _, row in df.iterrows():
        ts_iso = _to_iso_z(row["datetime"])

        total_power_mw = _safe_float(row.get("total_power_mw"))
        # ghi           = _safe_float(row.get("global_horizontal_irradiation_w_per_m2"))
        # amb_temp      = _safe_float(row.get("ambient_temperature_c"))
        # pv_temp       = _safe_float(row.get("pv_module_temperature_c"))

        # wind_speed      = _safe_float(row.get("wind_speed_m_per_s"))
        # wind_deg       = _safe_float(row.get("wind_direction_degree"))
        raw_ghi     = row.get("global_horizontal_irradiation_w_per_m2")
        raw_amb     = row.get("ambient_temperature_c")
        raw_pv      = row.get("pv_module_temperature_c")
        raw_wind_sp = row.get("wind_speed_m_per_s")
        raw_wind_de = row.get("wind_direction_degree")

        st_ghi,  ghi_val  = _status_and_value(raw_ghi)
        st_amb,  amb_val  = _status_and_value(raw_amb)
        st_pv,   pv_val   = _status_and_value(raw_pv)
        st_wsp,  wsp_val  = _status_and_value(raw_wind_sp)
        st_wdeg, wdeg_val = _status_and_value(raw_wind_de)


        status = "P"
        if total_power_mw is None or (isinstance(total_power_mw, float) and math.isnan(total_power_mw)):
            status = "E"
        elif total_power_mw <= 0:
            status = "NP"

        # actual gen record
        gen_rec = {
            "datetime": ts_iso,
            "value": total_power_mw,
            "plantcode": plantcode_label,
            "status": status,
            "activepercentage": af_lookup.get(ts_iso, 0.0)
        }
        total_response_actual_gen["data"].append(gen_rec)

        # weather records (use generic keys so they map via your key_mapping)
        # point="avg" since get_sa_features returns averaged weather for the site & 15-min bucket
        total_response_actual_weather["data"].append(
            make_weather_template(ts_iso, "global_horizontal_irradiation", "1", st_ghi,  ghi_val,  plantcode_label)
        )
        total_response_actual_weather["data"].append(
            make_weather_template(ts_iso, "ambient_temperature", "1", st_amb,  amb_val,  plantcode_label)
        )
        total_response_actual_weather["data"].append(
            make_weather_template(ts_iso, "pv_module_temperature", "1", st_pv,   pv_val,   plantcode_label)
        )
        total_response_actual_weather["data"].append(
            make_weather_template(ts_iso, "wind_speed_m_per_s", "1", st_wsp,  wsp_val,  plantcode_label)
        )
        total_response_actual_weather["data"].append(
            make_weather_template(ts_iso, "wind_direction_degree", "1", st_wdeg, wdeg_val, plantcode_label)
        )

    return total_response_actual_gen, total_response_actual_weather


def fetch_sites():
    """Read all sites from forecast_site: site_id, site_name, plant_code, code_number."""
    q = """
      SELECT site_id, site_name, plant_code, code_number
      FROM dbo.forecast_site
      ORDER BY site_id
    """
    with pyodbc.connect(CONN_AZ_SQL) as con:
        return pd.read_sql(q, con)


def get_active_percentage(conn_huawei: str, plant_code: str, start_dt, end_dt):
    sql = r"""
        WITH workingtime AS (
            SELECT
                DATEADD(MINUTE, DATEDIFF(MINUTE, 0, t0.[datetime]) / 15 * 15, 0) AS interval_time,
                t3.plantCode,
                COUNT(*) AS WorkingDay
            FROM [scgcehuawei].[dbo].[getDevRealKpi] AS t0
            INNER JOIN [scgcehuawei].[dbo].[getDevList]   AS t2 ON t0.devId = t2.id 
            INNER JOIN [scgcehuawei].[dbo].[stations]     AS t3 ON t2.stationCode COLLATE Thai_CI_AS = t3.plantCode COLLATE Thai_CI_AS
            WHERE
                t0.devId <> '1000000033963259'
                AND t0.[datetime] BETWEEN ? AND ?
                AND t3.plantCode = ?
            GROUP BY
                DATEADD(MINUTE, DATEDIFF(MINUTE, 0, t0.[datetime]) / 15 * 15, 0),
                t3.plantCode
        ),
        downtime AS (
            SELECT
                DATEADD(MINUTE, DATEDIFF(MINUTE, 0, t0.[datetime]) / 15 * 15, 0) AS interval_time,
                t3.plantCode,
                COUNT(*) AS Downtime
            FROM [scgcehuawei].[dbo].[getDevRealKpi] AS t0
            INNER JOIN [scgcehuawei].[dbo].[getDevList]   AS t2 ON t0.devId = t2.id
            INNER JOIN [scgcehuawei].[dbo].[stations]     AS t3 ON t2.stationCode COLLATE Thai_CI_AS = t3.plantCode COLLATE Thai_CI_AS
            WHERE
                t0.devId <> '1000000033963259'
                AND t0.inverter_state NOT IN (512, 513, 514)
                AND t0.[datetime] BETWEEN ? AND ?
                AND t3.plantCode = ?
            GROUP BY
                DATEADD(MINUTE, DATEDIFF(MINUTE, 0, t0.[datetime]) / 15 * 15, 0),
                t3.plantCode
        )
        SELECT
            wt.interval_time AS [time],
            ROUND(
                (CONVERT(FLOAT, wt.WorkingDay) - CONVERT(FLOAT, ISNULL(dt.Downtime, 0)))
                / NULLIF(CONVERT(FLOAT, wt.WorkingDay), 0) * 100.0, 2
            ) AS AF
        FROM workingtime AS wt
        LEFT JOIN downtime AS dt
          ON wt.interval_time = dt.interval_time
         AND wt.plantCode    = dt.plantCode
        ORDER BY wt.interval_time;
    """

    rows = []
    with pyodbc.connect(conn_huawei) as con:
        cur = con.cursor()
        cur.execute(sql, [start_dt, end_dt, plant_code, start_dt, end_dt, plant_code])
        for t, af in cur.fetchall():  # exactly two columns selected
            rows.append({"time": t, "AF": float(af) if af is not None else 0.0})
    return rows



def get_sa_features(plant_code: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    """
    Reuse the standardized cross-collation query to pull power + weather,
    then keep only FEATURES and timestamps within the day.
    """
    sql = r"""
        ;WITH map_project_code AS (
        SELECT
            [Project_Code]      AS project_code,
            [plantCode_huawei]  AS station_code,
            CAST([SerialNumberMeter] AS VARCHAR(50)) AS serial_number
        FROM EDMI.dbo.mapProjectCode
        WHERE plantCode_huawei IS NOT NULL AND Project_Code LIKE 'S0%'
    ),
    sn AS (
        SELECT CAST(m.SerialNumberMeter AS VARCHAR(50)) AS serial_number
        FROM EDMI.dbo.mapProjectCode AS m
        WHERE m.plantCode_huawei = ?
    ),
    total_power AS (
        SELECT
            pfwhim.Date_M AS [datetime],
            mpc.station_code,
            SUM(pfwhim.QtyValue * 4 * (dv.CTM / dv.CTD * dv.VTM / dv.VTD) / 1e6) AS total_power_mw
        FROM EDMI.dbo.tblProfileWhImp AS pfwhim
        JOIN EDMI.dbo.tblMeterPoints AS mtp ON pfwhim.MeterPointId = mtp.MeterPointId
        JOIN EDMI.dbo.tblDevices     AS dv  ON mtp.Code = dv.SerialNumber
        JOIN map_project_code        AS mpc ON mpc.serial_number = dv.SerialNumber
        WHERE dv.SerialNumber IN (SELECT serial_number FROM sn WHERE serial_number <> '250318201')
        AND pfwhim.Date_M BETWEEN ? AND ?
        GROUP BY mpc.station_code, pfwhim.Date_M

        UNION ALL

        SELECT
            pf.Date_M AS [datetime],
            mpc.station_code,
            SUM(pf.QtyValue * (dv.CTM / dv.CTD * dv.VTM / dv.VTD) / 1e6) AS total_power_mw
        FROM EDMI.dbo.tblProfile AS pf
        JOIN EDMI.dbo.tblMeterPoints AS mtp ON pf.MeterPointId = mtp.MeterPointId
        JOIN EDMI.dbo.tblDevices     AS dv  ON mtp.Code = dv.SerialNumber
        JOIN map_project_code        AS mpc ON mpc.serial_number = dv.SerialNumber
        WHERE dv.SerialNumber IN (SELECT serial_number FROM sn WHERE serial_number = '250318201')
        AND pf.Date_M BETWEEN ? AND ?
        AND pf.QtyType = 16403
        GROUP BY mpc.station_code, pf.Date_M
    ),
    weather_data AS (
        SELECT 
            em.Datetime AS [datetime],
            AVG(em.radiant_line)   AS ghi,
            AVG(em.temperature)    AS amb_temp,
            AVG(em.pv_temperature) AS pv_temp,
            AVG(em.wind_speed) AS wind_speed_m_per_s,
            AVG(em.wind_direction) AS wind_direction_degree,
            MAX(dl.stationCode)    AS station_code
        FROM scgcehuawei.dbo.getDevRealKpiEM AS em
        LEFT JOIN scgcehuawei.dbo.getDevList AS dl ON em.devId = dl.id
        WHERE dl.stationCode = ?
        AND em.Datetime BETWEEN ? AND ?
        AND DATEPART(MINUTE, em.Datetime) IN (0, 15, 30, 45)
        GROUP BY em.Datetime, dl.stationCode
    )
    SELECT 
        tp.[datetime],
        SUM(tp.total_power_mw) AS total_power_mw,
        AVG(wd.ghi)      AS global_horizontal_irradiation_w_per_m2,
        AVG(wd.amb_temp) AS ambient_temperature_c,
        AVG(wd.pv_temp)  AS pv_module_temperature_c,
        AVG(wd.wind_speed_m_per_s) AS wind_speed_m_per_s,
        AVG(wd.wind_direction_degree) AS wind_direction_degree
    FROM total_power AS tp
    LEFT JOIN weather_data AS wd
    ON tp.[datetime] = wd.[datetime]
    GROUP BY tp.[datetime]
    ORDER BY tp.[datetime] ASC;
    """
    params = [plant_code, start_dt, end_dt, start_dt, end_dt, plant_code, start_dt, end_dt]

    with pyodbc.connect(conn_str) as con:
        df = pd.read_sql(sql, con, params=params)

    if df.empty:
        return df

    df = df.infer_objects(copy=False)

    df["datetime"] = pd.to_datetime(df["datetime"])
    # clean fills to match training assumptions
    df["global_horizontal_irradiation_w_per_m2"] = df["global_horizontal_irradiation_w_per_m2"].fillna(0.0)
    df = df.infer_objects(copy=False)

    df["ambient_temperature_c"] = df["ambient_temperature_c"].interpolate(limit_direction="both")
    df["pv_module_temperature_c"] = df["pv_module_temperature_c"].interpolate(limit_direction="both")
    df["wind_speed_m_per_s"] = df["wind_speed_m_per_s"].interpolate(limit_direction="both")
    df["wind_direction_degree"] = df["wind_direction_degree"].interpolate(limit_direction="both")
    # print(df.to_string())
    # print(df)
    return df   


# API server URL (replace with your actual API URL)
API_SERVER_URL = os.environ.get("API_SERVER_URL", "http://localhost:8000/egat_test")

def forward_gen_data_to_EGAT(query_data):
    # try:
    #     # response = requests.post(url, json=data, headers=headers)
    #     response = requests.post("http://localhost:8000/egat_test", json=query_data)
    #     response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
    #     logger.info("API response:", response.json(),response) # or response.text
    # except requests.exceptions.RequestException as e:
    #     logger.error(f"Error forwarding query to API: {e}")

    #real server
    try:
        # response = requests.post(url, json=data, headers=headers)
        response = requests.post(f"{EGAT_BASE_URL}/actualgen/", json=query_data, headers=headers,timeout=(5, 20))
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        # logger.info(f"API response: {response.json()},{response}") # or response.text
        logger.info(f"API status: {response.status_code}, body preview: {str(response.text)[:200]}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error forwarding query to API: {e}")
        if e.response is not None:
            logger.error(f"Status code: {e.response.status_code}")
            logger.error(f"Response body: {e.response.text}")

def forward_weather_data_to_EGAT(query_data):
    # try:
    #     # response = requests.post(url, json=data, headers=headers)
    #     response = requests.post(f"http://localhost:8000/egat_test", json=query_data)
    #     response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
    #     logger.info("API response:", response.json(),response) # or response.text
    # except requests.exceptions.RequestException as e:
    #     logger.error(f"Error forwarding query to API: {e}")

    #real server
    try:
        # response = requests.post(url, json=data, headers=headers)
        response = requests.post(f"{EGAT_BASE_URL}/actualweather/", json=query_data, headers=headers,timeout=(5, 20))
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        # logger.info(f"API response: {response.json()},{response}") # or response.text
        logger.info(f"API status: {response.status_code}, body preview: {str(response.text)[:200]}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error forwarding query to API: {e}")
        if e.response is not None:
            logger.error(f"Status code: {e.response.status_code}")
            logger.error(f"Response body: {e.response.text}")


# 949999990006
def job():
    logger.info("Running job...")
    try:
        result = get_upload_server_config("949999990006")
        logger.info(f"Config result: {result}")
        
        if not result or len(result) < 2:
            logger.error(f"Unexpected config result: {result}")
            return
        
        total_response_actual_gen, total_response_actuat_weather = result
        
        forward_gen_data_to_EGAT(total_response_actual_gen)
        forward_weather_data_to_EGAT(total_response_actuat_weather)
        
        logger.info("Job finished.")
    except Exception as e:
        logger.error(f"Job not pass: {e}")

from datetime import datetime, timedelta

def get_features_window(features_day: str):
    now = datetime.now()

    # Round DOWN to nearest 15-minute interval
    minute = (now.minute // 15) * 15
    rounded_now = now.replace(minute=minute, second=0, microsecond=0)

    endtime = rounded_now
    starttime = endtime - timedelta(days=1)

    starttime_str = starttime.strftime("%Y-%m-%d %H:%M:%S")
    endtime_str = endtime.strftime("%Y-%m-%d %H:%M:%S")

    return starttime_str, endtime_str



if __name__ == "__main__":
    sites = fetch_sites()
    sites = sites[sites["site_id"] >= 3].copy()
    sites = sites[sites["site_id"] < 5].copy()
    # print(sites)
    
    start_str, end_str = get_features_window("ignored")
    
    # af = get_active_percentage(conn_str,"NE=33975124",start_str, end_str)
    # print(af)
    for _, s in sites.iterrows():
        plant_code = s["plant_code"]              # e.g., NE=33975124
        label      = s["site_name"] or plant_code # optional nicer label
        # df = get_sa_features(plant_code=plant_code, start_dt=start_str, end_dt=end_str)
        # print(df)
        # af = get_active_percentage(conn_str,plant_code,start_str, end_str)
        # print(af)
        gen, weather = build_upload_payloads(
            conn_str=conn_str,
            plant_code=plant_code,
            start_dt=start_str,
            end_dt=end_str,
            plantcode_label=label
        )
        # print(weather)
        try:            
            logger.info("Start forwarding.")
            forward_gen_data_to_EGAT(gen)
            forward_weather_data_to_EGAT(weather)
            logger.info("Job finished.")
            # logger.info("Scheduler started. Running every 15 minutes.")
        except Exception as e:
            logger.error(f"Job not pass{e}")
       