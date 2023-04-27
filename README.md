### Problem / Overview of the Project
A simple project which gets the forecasted and current temperature for the cities situated in Mindanao island Philippines.  The comparison of temperatures among cities (forecasted and current) per day is then visualized in the dashboard.

### Dataset
The dataset is created by fetching the current and forecasted data from the OpenWeather API.

### Dashboard
You may access the dashboard using this [link](https://lookerstudio.google.com/s/oipctNcA_Ps)

### Project details and Implementation
Prefect Cloud and Docker were used for this project. Services in the Google Cloud Platform were also used and these are BigQuery, Cloud Storage, and VM Instances.  
Prefect Cloud was used in orchestrating these processes:
- fetching weather data from the OpenWeather API
- flattening and enhancing the response data
- Push it to the target Cloud Storage bucket
- Save the data to the target BigQuery table

<br/>The deployed flows used Github as the code storage location and the Docker container as an infrastructure to execute the workflows.
<br/>The Cloud Storage bucket serves as the Data Lake and BigQuery as the data warehouse.
<br/>Scheduled queries were made in BigQuery to create tables for summary or reporting.
<br/>The visualization dashboard is a simple Google Data Studio report with 4 tiles.

### Reproduce the Project / Resources
To Learn more on how to reproduce the Project and its in-depth documentation Please follow this [link](https://docs.google.com/document/d/1gcMDJIsQqtdwnAYf4MF6qSEA6BKDRsrInzWW-ydzvEU/edit?usp=sharing)


### Relevant Files explained
| Plugin | README |
| ------ | ------ |
| cities in mindanao.csv | Contains the target cities to be extracted|
| docker-requirements.txt | Contains libraries needed for the docker container |
| current_weather_etl.py | Prefect flow for orchestrating ETL of current weather data |
| forecast_weather_etl.py | Prefect flow for orchestrating ETL of forecast weather data |
| scheduled query.sql | SQL queries used in the project |

