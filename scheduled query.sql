#  query for creating partitioned tables for current_test
CREATE TABLE sample_weather.current_test_partitioned
PARTITION BY date(date_unix)
AS
SELECT * FROM sample_weather.current_test;

DROP TABLE sample_weather.current_test;
 
ALTER TABLE sample_weather.current_test_partitioned
RENAME TO current_test;


#  query for creating partitioned tables for forecast_test
CREATE TABLE sample_weather.forecast_test_partitioned
PARTITION BY date(date_text)
AS
SELECT * FROM sample_weather.forecast_test;

DROP TABLE sample_weather.forecast_test;
 
ALTER TABLE sample_weather.forecast_test_partitioned
RENAME TO forecast_test;


# Scheduled query for forecast 
With city as (
  Select ST_GEOGPOINT(round(longitude,4), round(latitude, 4)) as city_point,
  concat(Latitude, ", ", Longitude) as city_coord,
  Name
  from `level-agent-375808.sample_weather.target_cities`
)

  Select 
  city.city_coord,
  city.name,
  Date(date_text) as date,
  if(part_of_day='d', 'day', 'night') as part_of_day,
  round(avg(main_temp),2) as average_temp,
  round(avg(precipitation_probability)*100, 2) as avg_precipitation_probability
  from `level-agent-375808.sample_weather.forecast_test` forecast
  join city on ST_EQUALS(city.city_point, ST_GEOGPOINT(forecast.longitude, forecast.latitude))
  group by city.name,
  city.city_coord,
  date,
  part_of_day


# Scheduled queries forecast vs current temp
With city as (
  Select ST_GEOGPOINT(round(longitude,4), round(latitude, 4)) as city_point,
  concat(Latitude, ", ", Longitude) as city_coord,
  Name
  from `level-agent-375808.sample_weather.target_cities`
)

  Select 
  city.city_coord,
  city.name,
  Date(date_unix) as date,
  round(avg(main_temp),2) as average_current_temp,
  avg(forecast_temp.average_temp) as average_forecast_temp,
  round(avg(forecast_temp.average_temp) - avg(main_temp), 2) as difference
  from `level-agent-375808.sample_weather.current_test` forecast
  join city on ST_EQUALS(city.city_point, ST_GEOGPOINT(forecast.longitude, forecast.latitude))
  inner join `level-agent-375808.sample_weather.forecast_avg_temperature` forecast_temp on city.name=forecast_temp.name and
  forecast_temp.date=Date(forecast.date_unix)
  group by city.name,
  city.city_coord,
  date
