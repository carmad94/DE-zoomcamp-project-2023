import requests
import json
import pandas as pd
from prefect import flow, task
from datetime import datetime, date
from pathlib import Path
from prefect.blocks.system import Secret
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import bigquery_load_cloud_storage, bigquery_query
from prefect_gcp import GcpCredentials

@task(retries=3, log_prints=True)
def fetch(forecast_url: str) -> json:
    forecast_request = requests.get(forecast_url)
    if forecast_request.status_code==200:
        return forecast_request.json()


@task(log_prints=True)
def flatten(forecast_json: json) -> pd.DataFrame:
    target_columns = [
        'dt',
        'dt_txt',
        'visibility',
        ['main', 'temp'],
        ['main', 'feels_like'],
        ['main', 'temp_min'],
        ['main', 'temp_max'],
        ['main', 'pressure'],
        ['main', 'sea_level'],
        ['main', 'grnd_level'],
        ['main', 'humidity'],
        ['main', 'temp_kf'],
        'pop',
        ['clouds', 'all'],
        ['rain', '3h'],
        ['sys', 'pod'],
        ['wind', 'speed'],
        ['wind', 'deg'],
        ['wind', 'gust']
    ]
    forecast_df = pd.json_normalize(
        forecast_json['list'],
        'weather',
        target_columns,
        errors='ignore',
        record_prefix='main_'
    )
    return forecast_df


@task(log_prints=True)
def enhance(df: pd.DataFrame, forecast_json: json, datetime_extracted: str) -> Path:
    city_details = forecast_json['city']
    df['city'] = city_details['name']
    df['latitude'] = city_details['coord']['lat']
    df['longitude'] = city_details['coord']['lon']
    df['country'] = city_details['country']
    df['timezone'] = city_details['timezone']
    df['sunrise'] = datetime.utcfromtimestamp(
        city_details['sunrise']
    ).strftime('%Y-%m-%d %H:%M:%S')
    df['sunset'] = datetime.utcfromtimestamp(
        city_details['sunset']
    ).strftime('%Y-%m-%d %H:%M:%S')
    df['dt'] = pd.to_datetime(df['dt'],unit='s')
    df['datetime_extracted'] = datetime_extracted
    df.columns = df.columns.str.replace(".", "_", regex=False)
    
    # rename columns to make it more understandable
    column_names = {
        'sys_pod': 'part_of_day',
        'dt': 'date_unix',
        'dt_txt': 'date_text',
        'pop': 'precipitation_probability',
        'wind_deg': 'wind_direction',
        'main_main': 'weather',
        'main_sea_level': 'sea_level_pressure',
        'main_grnd_level': 'ground_level_pressure'
    }
    df.rename(columns=column_names, inplace=True)
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame) -> Path:
    today = date.today()
    date_extracted = today.strftime("%Y-%m-%d")
    datetime_extracted = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    datetime_extracted = datetime_extracted.replace(':', '')
    path_name = f"data/forecast/{date_extracted}/{datetime_extracted}.json"
    path = Path(f"data/forecast/{date_extracted}").mkdir(
        parents=True,
        exist_ok=True
    )
    path = Path(path_name)
    df.to_json(
        path,
        orient="records",
        lines=True,
        date_format="iso"
    )
    return path


@task(log_prints=True)
def write_gcs(path: Path)->None:
    gcs_block = GcsBucket.load("test-weather-bucket")
    to_path = path.as_posix()
    print(f"Uploading {to_path}")
    gcs_block.upload_from_path(from_path=path, to_path=to_path)
    return


def write_bq(path: Path, table: str) -> None:
    print("Write BQ")
    gcp_credentials_block = GcpCredentials.load("level-agent-375808")
    gcp_credentials = gcp_credentials_block.get_credentials_from_service_account()
    gcs_block = GcsBucket.load("test-weather-bucket")
    bucket_name = gcs_block.get_bucket().name
    to_path = path.as_posix()
    uri = f"gs://{bucket_name}/{to_path}"
    print(uri)
    job_config = {
        "autodetect": True,
        "source_format": 'NEWLINE_DELIMITED_JSON',
        "create_disposition": 'CREATE_IF_NEEDED',
        "write_disposition": 'WRITE_APPEND',
        "schema_update_options": 'ALLOW_FIELD_ADDITION',
        "max_bad_records": 200
    }
    result = bigquery_load_cloud_storage(
        dataset="sample_weather",
        table=table,
        uri=uri,
        gcp_credentials=gcp_credentials_block,
        job_config=job_config
    )
    return result


@flow()
def process_forecast(latitude: float, longitude: float) -> pd.DataFrame:
    weather_api_block = Secret.load("openweather-api")
    weather_api_key=weather_api_block.get()
    base_url="https://api.openweathermap.org/data/2.5/"
    

    """The main ETL function"""
    # latitude = 7.0648306
    # longitude = 125.6080623
    forecast_url = f"{base_url}/forecast?lat={latitude}&lon={longitude}&appid={weather_api_key}&units=metric"
    datetime_extracted = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    forecast_json = fetch(forecast_url)
    print(forecast_json['city'])
    df_forecast = flatten(forecast_json)
    df_forecast = enhance(
        df_forecast,
        forecast_json,
        datetime_extracted
    )
    return df_forecast


def get_target_cities():
    gcp_credentials_block = GcpCredentials.load("level-agent-375808")
    gcp_credentials = gcp_credentials_block.get_credentials_from_service_account()
    query = "SELECT * FROM `sample_weather.target_cities` LIMIT 10"
    query_result = bigquery_query(
        query,
        gcp_credentials_block,
        to_dataframe=True
    )
    print(query_result)
    return query_result


@flow()
def etl_parent_flow():
    df_cities=get_target_cities()
    report_df = None
    for row in df_cities.itertuples():
        print(row.Name)
        df_forecast = process_forecast(row.Latitude, row.Longitude)
        if (report_df is not None and isinstance(report_df, pd.DataFrame)) and \
                    (df_forecast is not None and isinstance(df_forecast, pd.DataFrame)):
                    # report_df = report_df.append(df)
            report_df = pd.concat([report_df,df_forecast], axis=0, ignore_index=True)
        else:
            report_df = df_forecast
    path = write_local(report_df)
    write_gcs(path)
    write_bq(path, "forecast_test")


if __name__ == "__main__":
    # etl_web_to_gcs()
    etl_parent_flow()

    