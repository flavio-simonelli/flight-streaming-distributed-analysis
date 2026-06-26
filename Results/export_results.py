import os
import json
from datetime import datetime, timedelta
import pandas as pd
from influxdb_client import InfluxDBClient

# ==========================================
# INFLUXDB CONNECTION CONFIGURATION
# ==========================================
URL = "http://influxdb.flight-analysis.local:8086"
TOKEN = "apiv3_5C0GLRl9n4gviNmyGc4wtlALsMEaP804VBIJlLvSvBjxDB0_1QHiC8B6_i8kxnkRryAnXW8f9uSn9HANm1_5cA"
ORG = "sae-org"
BUCKET = "flights-data"

OUTPUT_DIR = "."
os.makedirs(OUTPUT_DIR, exist_ok=True)

client = InfluxDBClient(url=URL, token=TOKEN, org=ORG)
query_api = client.query_api()

def format_dataframe_time(df, time_column="_time"):
    """Formats InfluxDB timestamps into the requested standard format."""
    if df.empty or time_column not in df.columns:
        return df
    df[time_column] = pd.to_datetime(df[time_column])
    df[time_column] = df[time_column].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

def parse_and_format_flights(json_str):
    """Converts a standard JSON array string into the required [(carrier,dest,delay), ...] format."""
    if not json_str or pd.isna(json_str) or json_str == "[]":
        return "[]"
    try:
        flights_list = json.loads(str(json_str))
        if not isinstance(flights_list, list):
            return "[]"

        tuple_strings = []
        for f in flights_list:
            carrier = f.get("carrier", "")
            dest = f.get("dest", "")
            delay = float(f.get("dep_delay", 0.0))
            tuple_strings.append(f"({carrier},{dest},{delay:.2f})")

        return f"[{', '.join(tuple_strings)}]"
    except Exception:
        return "[]"

# ==========================================
# EXPORT QUERY 1
# ==========================================
def export_query1():
    print("Exporting Query 1...")
    flux_query = f'''
    from(bucket: "{BUCKET}")
      |> range(start: 0)
      |> filter(fn: (r) => r["_measurement"] == "flights_q1_results")
      |> pivot(rowKey:["_time", "airline"], columnKey: ["_field"], valueColumn: "_value")
    '''
    df = query_api.query_data_frame(flux_query)

    if not df.empty:
        df['_time'] = pd.to_datetime(df['_time'])
        df['window_end'] = df['_time'] + timedelta(hours=1)

        df['window_start'] = df['_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['window_end'] = df['window_end'].dt.strftime('%Y-%m-%d %H:%M:%S')

        columns_order = [
            'window_start', 'window_end', 'airline', 'num_flights',
            'completed', 'cancelled', 'diverted', 'dep_delay_mean',
            'cancellation_rate', 'late_departure_rate'
        ]

        for col in columns_order:
            if col not in df.columns:
                df[col] = 0

        df_final = df[columns_order]
        df_final.to_csv(os.path.join(OUTPUT_DIR, "query1.csv"), index=False)
        print("Query 1 exported successfully.")
    else:
        print("No data found for Query 1.")

# ==========================================
# EXPORT QUERY 2
# ==========================================
def export_query2():
    windows = ["1h", "6h", "global"]
    for w in windows:
        print(f"Exporting Query 2 (Window: {w})...")
        flux_query = f'''
        from(bucket: "{BUCKET}")
          |> range(start: 0)
          |> filter(fn: (r) => r["_measurement"] == "flights_q2_results" and r["window_type"] == "{w}")
          |> pivot(rowKey:["_time", "rank", "window_type"], columnKey: ["_field"], valueColumn: "_value")
        '''
        df = query_api.query_data_frame(flux_query)

        if not df.empty:
            df = format_dataframe_time(df, "_time")
            df.rename(columns={"_time": "ts"}, inplace=True)

            if "delayed_flights" in df.columns:
                df["delayed_flights"] = df["delayed_flights"].apply(parse_and_format_flights)

            columns_order = [
                'ts', 'rank', 'origin_airport_id', 'num_flights',
                'severe_delays', 'dep_delay_mean', 'dep_delay_max', 'delayed_flights'
            ]

            for col in columns_order:
                if col not in df.columns:
                    df[col] = ""

            df_final = df[columns_order].sort_values(by=['ts', 'rank'])
            file_path = os.path.join(OUTPUT_DIR, f"query2_{w}.csv")
            df_final.to_csv(file_path, index=False)

            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            content = content.replace('"[', '[').replace(']"', ']')
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)

            print(f"Query 2 ({w}) exported successfully.")
        else:
            print(f"No data found for Query 2 ({w}).")

# ==========================================
# EXPORT QUERY 3
# ==========================================
def export_query3():
    windows = ["1d", "7d", "global"]
    for w in windows:
        print(f"Exporting Query 3 (Window: {w})...")
        flux_query = f'''
        from(bucket: "{BUCKET}")
          |> range(start: 0)
          |> filter(fn: (r) => r["_measurement"] == "flights_q3_results" and r["window_type"] == "{w}")
          |> pivot(rowKey:["_time", "airline", "window_type", "hour"], columnKey: ["_field"], valueColumn: "_value")
        '''
        df = query_api.query_data_frame(flux_query)

        if not df.empty:
            df = format_dataframe_time(df, "_time")
            df.rename(columns={"_time": "ts"}, inplace=True)

            columns_order = [
                'ts', 'airline', 'hour', 'count', 'min',
                'p25', 'p50', 'p75', 'p90', 'max'
            ]

            for col in columns_order:
                if col not in df.columns:
                    df[col] = 0

            df["hour"] = pd.to_numeric(df["hour"]).astype(int)

            df_final = df[columns_order].sort_values(by=['ts', 'hour', 'airline'])
            df_final.to_csv(os.path.join(OUTPUT_DIR, f"query3_{w}.csv"), index=False)
            print(f"Query 3 ({w}) exported successfully.")
        else:
            print(f"No data found for Query 3 ({w}).")

# ==========================================
# RUN EXPORT FOR ALL DATASETS
# ==========================================
if __name__ == "__main__":
    export_query1()
    export_query2()
    export_query3()
    client.close()
    print("\nExport process completed. Files are saved in the output directory.")