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

def connect_database() -> None:
    pipeline.get_connection()
    pipeline.valid_today_execution()

def create_sql_objects() -> None:
    pipeline.create_sql_objects()

def run_etl() -> None:
    pipeline.etl_process()

def close_connection() -> None:
    pipeline.close_connection()