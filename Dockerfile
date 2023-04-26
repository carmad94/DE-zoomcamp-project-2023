FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY forecast_weather_etl.py /opt/prefect/flows/forecast_weather_etl.py
COPY current_weather_etl.py /opt/prefect/flows/test_current_weather_etl.py
RUN mkdir -p /opt/prefect/data/forecast
RUN mkdir -p /opt/prefect/data/current