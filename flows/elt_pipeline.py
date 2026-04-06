from prefect import flow, get_run_logger
from flows.raw_to_stage import raw_to_stage_flow
from flows.stage_to_dwh import stage_to_dwh_flow


@flow(name="elt-pipeline", log_prints=True)
def elt_pipeline_flow(config_path: str = "config/config.yaml"):
    """Full ELT pipeline: raw -> staging -> DWH"""
    logger = get_run_logger()

    logger.info("Step 1/2: Loading raw data into staging...")
    raw_to_stage_flow(config_path=config_path)

    logger.info("Step 2/2: Transforming staging to DWH...")
    stage_to_dwh_flow(config_path=config_path)

    logger.info("ELT pipeline completed successfully")
