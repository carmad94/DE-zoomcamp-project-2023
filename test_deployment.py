from prefect.deployments import Deployment
from prefect_gcp.cloud_storage import GcsBucket
from test_etl import etl_parent_flow

gcs_block = GcsBucket.load("test-weather-bucket")
docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="fetch-weather-forecast-test",
    version=1,
    work_queue_name="test",
    storage=gcs_block
)


if __name__ == "__main__":
    docker_dep.apply()
    #prefect deployment build test_etl.py:etl_parent_flow -n fetch-weather-forecast-test -sb gcs/test-gcs -q test -o test-weather-forecast-deployment.yaml --cron "*/10 * * * *"