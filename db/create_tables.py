import psycopg2
from db import DatabaseConection

class CreateTables(DatabaseConection):
    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self.schema = self.config.get('REDSHIFT_SCHEMA')

    def create_stage_table_executionLog(self) -> None:
        """ Create a table in the database """
        table_name = 'stg_executionLog'
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{table_name} (
                    id INT IDENTITY(1,1),
                    last_execution DATE NOT NULL
                );
                """)
                self.conn.commit()
                self.print_table_message(table_name, 1)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.print_table_message(table_name, 2)
            self.conn.rollback()
            exit()

    def create_stage_table_medallero(self) -> None:
        """ Create stage table in the database """
        table_name = 'stg_medallero'
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{table_name} (
                    id INT IDENTITY(1,1),
                    rank INT,
                    country VARCHAR(255),
                    gold INT,
                    silver INT,
                    bronze INT,
                    total INT,
                    execution_date DATE NOT NULL
                );
                """)
                self.conn.commit()
                self.print_table_message(table_name, 1)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.print_table_message(table_name, 2)
            self.conn.rollback()
            exit()

    def create_edw_table_medallero(self) -> None:
        """ Create edw table in the database """
        table_name = 'edw_medallero'
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{table_name} (
                    id INT IDENTITY(1,1),
                    rank INT,
                    country VARCHAR(255),
                    gold INT,
                    silver INT,
                    bronze INT,
                    total INT,
                    execution_date DATE not null,
                    is_active INT not null,
                    country_id INT
                );
                """)
                self.conn.commit()
                self.print_table_message(table_name, 1)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.print_table_message(table_name, 2)
            self.conn.rollback()
            exit()

    def create_stage_table_countries(self) -> None:
        """ Create stage table in the database """
        table_name = 'stg_countries'
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{table_name} (
                    id INT IDENTITY(1,1),
                    country VARCHAR(255),
                    alpha2code VARCHAR(2),
                    alpha3code VARCHAR(3),
                    numeric VARCHAR(3)
                );
                """)
                self.conn.commit()
                self.print_table_message(table_name, 1)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.print_table_message(table_name, 2)
            self.conn.rollback()
            exit()

    def create_edw_table_countries(self) -> None:
        """ Create Country table in the database """
        table_name = 'edw_Countries'
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{table_name} (
                    id INT IDENTITY(1,1),
                    country VARCHAR(255) NOT NULL
                );
                """)
                self.conn.commit()
                self.print_table_message(table_name, 1)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.print_table_message(table_name, 2)
            self.conn.rollback()
            exit()
    
    def create_edw_view_medallero(self) -> None:
        """ Create edw view in the database """
        view_name = 'edw_medallero_view'
        table_name = 'edw_medallero'
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"""
                CREATE OR REPLACE VIEW {self.schema}.{view_name}
                AS SELECT
                    rank as "Rank"
                    ,country as "Country"
                    ,gold as "Gold"
                    ,silver as "Silver"
                    ,bronze as "Bronze"
                    ,total as "Total"
                    ,execution_date as "Last Update"
                FROM {self.schema}.{table_name}
                where is_active = 1
                order by rank ;
                """)
                self.conn.commit()
                self.print_table_message(view_name, 1)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            self.print_table_message(view_name, 2)
            self.conn.rollback()
            exit()