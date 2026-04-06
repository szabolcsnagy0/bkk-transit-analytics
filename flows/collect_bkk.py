from prefect import flow, task
from src.bkk_collector import BKKCollector


@task(retries=3, retry_delay_seconds=[5, 10, 20], log_prints=True)
def collect_vehicles(config_path: str = "config/config.yaml") -> bool:
    collector = BKKCollector(config_path=config_path)
    return collector.run()


@flow(name="collect-bkk-vehicles", log_prints=True)
def collect_bkk_flow(config_path: str = "config/config.yaml"):
    success = collect_vehicles(config_path=config_path)
    if not success:
        raise Exception("BKK collection failed")
