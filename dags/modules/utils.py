from .data_pipeline import DataPipeline

CREATE_TABLES = True
CREATE_VIEWS = True
EXTRACT_COUNTRIES = True
RUN_ETL = True

pipeline = DataPipeline(
    create_tables=CREATE_TABLES,
    create_views=CREATE_VIEWS,
    extract_countries=EXTRACT_COUNTRIES,
    run_etl=RUN_ETL
)

def create_sql_objects() -> None:
    pipeline.get_connection()
    pipeline.valid_today_execution()
    pipeline.create_sql_objects()
    pipeline.close_connection()

def run_etl() -> None:
    pipeline.get_connection()
    pipeline.valid_today_execution()
    pipeline.etl_process()
    pipeline.close_connection()
