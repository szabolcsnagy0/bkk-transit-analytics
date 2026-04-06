from prefect import flow, task
from src.stage_to_dwh import ETLStageToDWH


@task(log_prints=True)
def populate_dim_time(config_path: str = "config/config.yaml"):
    etl = ETLStageToDWH(config_path=config_path)
    etl.populate_dim_time()


@task(log_prints=True)
def populate_dim_weather(config_path: str = "config/config.yaml"):
    etl = ETLStageToDWH(config_path=config_path)
    etl.populate_dim_weather()


@task(log_prints=True)
def populate_dim_vehicle(config_path: str = "config/config.yaml"):
    etl = ETLStageToDWH(config_path=config_path)
    etl.populate_dim_vehicle()


@task(log_prints=True, timeout_seconds=600)
def calculate_delays_and_load_facts(config_path: str = "config/config.yaml"):
    etl = ETLStageToDWH(config_path=config_path)
    etl.calculate_delays_and_load_facts()


@flow(name="stage-to-dwh", log_prints=True)
def stage_to_dwh_flow(config_path: str = "config/config.yaml"):
    populate_dim_time(config_path=config_path)
    populate_dim_weather(config_path=config_path)
    populate_dim_vehicle(config_path=config_path)
    calculate_delays_and_load_facts(config_path=config_path)
