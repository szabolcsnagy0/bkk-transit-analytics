from prefect import flow, task, get_run_logger
from src.weather_collector import UnifiedWeatherCollector


@task(retries=2, retry_delay_seconds=30, log_prints=True)
def collect_weather_data(days_back: int = 2, config_path: str = "config/config.yaml"):
    collector = UnifiedWeatherCollector(config_path=config_path)
    collector.collect_weather(days_back=days_back, mode_name="Prefect Collection")
    return collector.collected_count, collector.failed_count


@flow(name="collect-weather", log_prints=True)
def collect_weather_flow(days_back: int = 2, config_path: str = "config/config.yaml"):
    logger = get_run_logger()
    collected, failed = collect_weather_data(days_back=days_back, config_path=config_path)
    logger.info(f"Weather collection: {collected} collected, {failed} failed")
    if failed > 0 and collected == 0:
        raise Exception("Weather collection completely failed")
