from prefect import flow, task
from src.raw_to_stage import ETLRawToStage


@task(log_prints=True)
def truncate_staging(config_path: str = "config/config.yaml"):
    etl = ETLRawToStage(config_path=config_path)
    etl.truncate_staging_tables()


@task(retries=1, retry_delay_seconds=10, log_prints=True)
def load_vehicles_to_staging(config_path: str = "config/config.yaml"):
    etl = ETLRawToStage(config_path=config_path)
    etl.load_vehicles()


@task(retries=1, retry_delay_seconds=10, log_prints=True)
def load_weather_to_staging(config_path: str = "config/config.yaml"):
    etl = ETLRawToStage(config_path=config_path)
    etl.load_weather()


@flow(name="raw-to-stage", log_prints=True)
def raw_to_stage_flow(config_path: str = "config/config.yaml"):
    truncate_staging(config_path=config_path)
    # After truncation, load vehicles and weather in parallel
    v_future = load_vehicles_to_staging.submit(config_path=config_path)
    w_future = load_weather_to_staging.submit(config_path=config_path)
    v_future.result()
    w_future.result()
