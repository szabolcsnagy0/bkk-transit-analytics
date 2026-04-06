from prefect import flow, task
from src.gtfs_loader import GTFSLoader


@task(log_prints=True)
def load_gtfs_staging(config_path: str = "config/config.yaml"):
    loader = GTFSLoader(config_path=config_path)
    loader.load_staging_tables()


@task(log_prints=True)
def populate_gtfs_dimensions(config_path: str = "config/config.yaml"):
    loader = GTFSLoader(config_path=config_path)
    loader.populate_dimensions()


@flow(name="load-gtfs", log_prints=True)
def load_gtfs_flow(config_path: str = "config/config.yaml"):
    load_gtfs_staging(config_path=config_path)
    populate_gtfs_dimensions(config_path=config_path)
