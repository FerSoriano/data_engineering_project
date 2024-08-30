import os
from datetime import datetime
from dotenv import load_dotenv

from .db import Database
from .etl import Countries, Medals

load_dotenv()

class DataPipeline():
    def __init__(self, create_tables: bool, create_views: bool, extract_countries: bool, run_etl: bool) -> None:
        self.create_tables = create_tables
        self.create_views = create_views
        self.extract_countries = extract_countries
        self.run_etl = run_etl

        self.config = {
            "REDSHIFT_USERNAME" : os.getenv('REDSHIFT_USERNAME'),
            "REDSHIFT_PASSWORD" : os.getenv('REDSHIFT_PASSWORD'),
            "REDSHIFT_HOST" : os.getenv('REDSHIFT_HOST'),
            "REDSHIFT_PORT" : os.getenv('REDSHIFT_PORT', '5439'),
            "REDSHIFT_DBNAME" : os.getenv('REDSHIFT_DBNAME'),
            "REDSHIFT_SCHEMA" : os.getenv('REDSHIFT_SCHEMA'), 
            }
        
        self.db = Database(config=self.config)
        self.countries = Countries()
        self.medals = Medals()

# 1- Validar Conexion
    def get_connection(self) -> None:
        """
        Get Database connetion.
        """
        self.db.get_conn()
        
    def valid_today_execution(self) -> None:
        """
        Validate if the ETL process was executed today.
        """
        last_execution = self.db.get_last_execution()
        self.today = datetime.now().strftime("%Y-%m-%d")

        if self.today == str(last_execution):
            print('Execution completed, the process has already been executed today. Data not added.')
            self.db.close_conn()
            exit()
        
# 2 - Crear objetos SQL
    def create_sql_objects(self) -> None:
        """
        Create tables and views
        """
        if self.create_tables:
            self.db.create_stage_table_executionLog()
            self.db.create_stage_table_medallero()
            self.db.create_stage_table_countries()
            self.db.create_edw_table_countries()
            self.db.create_edw_table_medallero()
        
        if self.create_views:
            self.db.create_edw_view_medallero()

# 3 - Extraer, Transformar y Cargar datos
    def etl_process(self) -> None:
        """
        Extract, Transform, Load
        """
        if self.medals.valid_dates(self.today):

            if self.extract_countries:
                self.countries.get_response()
                df_countries = self.countries.create_df()
                self.db.insert_to_stage_table_countries(df=df_countries)

            if self.run_etl:
                self.medals.get_response()
                df = self.medals.get_medals(self.today)
                # Truncar Stage
                self.db.truncate_stage_table_medallero()
                # Insertar Stage
                self.db.insert_to_stage_table_medallero(df=df)
                # Insertar Execution Log
                self.db.insert_to_stage_table_executionlog(self.today)
                # Insertar Stage
                self.db.insert_to_edw_table_countries()
                # Actualizar Is_Active a 0
                self.db.update_edw_table_medallero()
                # Insertar en EDW
                self.db.insert_to_edw_table_medallero()
                
                print('ETL process completed successfully...')
        else:
            print('The Olympic Games have ended ):')
            self.db.close_conn()
            exit()

# 4 - Close Connection
    def close_connection(self) -> None:
        """
        Close Database connection
        """
        self.db.close_conn()

if __name__ == "__main__":
    etl = DataPipeline()
    etl.get_connection()
    etl.valid_today_execution()
    etl.create_sql_objects()
    etl.etl_process()
    etl.close_connection()