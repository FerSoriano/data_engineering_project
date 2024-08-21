# DML = Data Manipulation Language

import psycopg2
from db import CreateTables

class DataManipulation(CreateTables):
    def __init__(self, config: dict) -> None:
        super().__init__(config)

    def insert_to_stage_table_medallero(self, df) -> None:
        """ Insert DataFrame into Redshift table """
        table_name = 'stg_medallero'
        try:
            with self.conn.cursor() as cursor:
                for index, row in df.iterrows():
                    cursor.execute(f"""
                        INSERT INTO {self.schema}.{table_name} (rank, country, gold, silver, bronze, total, execution_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (index, row['Country'], row['Gold'], row['Silver'], row['Bronze'], row['Total'], row['execution_date']))
                self.conn.commit()
                print(f"Data inserted successfully into {table_name}.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            print('Error: insert_to_stage_table_medallero()')
            self.conn.rollback()
            exit()
    
    def insert_to_stage_table_countries(self, df) -> None:
        """ Insert DataFrame into Redshift table """
        table_name = 'stg_countries'
        try:
            with self.conn.cursor() as cursor:
                for index, row in df.iterrows():
                    cursor.execute(f"""
                        INSERT INTO {self.schema}.{table_name} (country, alpha2code, alpha3code, numeric)
                        SELECT %s, %s, %s, %s
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM {self.schema}.{table_name}
                            WHERE country = %s 
                        );
                    """, (row['Country'], row['Alpha-2 code'], row['Alpha-3 code'], row['Numeric'], row['Country']))
                self.conn.commit()
                print(f"Data inserted successfully into {table_name}.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            print('Error: insert_to_stage_table_countries()')
            self.conn.rollback()
            exit()

    def insert_to_stage_table_executionlog(self, last_execution) -> None:
        """ Insert last execution date """
        table_name = 'stg_executionlog'
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"""
                        INSERT INTO {self.schema}.{table_name} (last_execution)
                        VALUES (%s)
                    """, (last_execution,))
                self.conn.commit()
                print("Data inserted successfully into stage_executionLog.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            print('Error: insert_to_stage_table_executionlog()')
            self.conn.rollback()
            exit()
    
    def insert_to_edw_table_medallero(self) -> None:
        """ Insert into EDW table """
        insert_table_name = 'edw_medallero'
        from_table_name = 'stg_medallero'
        left_table_name = 'edw_countries'
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO {self.schema}.{insert_table_name}(
                            rank,
                            country,
                            gold,
                            silver,
                            bronze,
                            total,
                            execution_date,
                            is_active,
                            country_id
                        )
                            SELECT
                                rank, 
                                m.country,
                                gold,
                                silver,
                                bronze,
                                total,
                                execution_date,
                                1 as is_active,
                                c.id as "country_id"
                            FROM {from_table_name} m
                            LEFT JOIN {left_table_name} c 
		                        ON c.country = m.country;
                    """)
                self.conn.commit()
                print("Data inserted successfully into edw_medallero.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            print('Error: insert_to_edw_table_medallero()')
            self.conn.rollback()
            exit()

    def insert_to_edw_table_countries(self) -> None:
        """ Insert into EDW table """
        table_name = 'edw_Countries'
        from_table_name = 'stg_medallero'
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO {self.schema}.{table_name} (country)
                    SELECT DISTINCT country 
                    FROM {from_table_name} m
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM {table_name} c
                        WHERE c.country = m.country
                        )
                    ORDER BY 1 ASC;
                    """)
                self.conn.commit()
                print("Data inserted successfully into edw_countries.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            print('Error: insert_to_edw_table_countries()')
            self.conn.rollback()
            exit()

    def truncate_stage_table_medallero(self) -> None:
        """ Truncate stage table """
        table_name = 'stg_medallero'
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE table {self.schema}.{table_name};")
                self.conn.commit()
                print("truncate_stage_table_medallero() -> executed successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            print('Error: truncate_stage_table_medallero()')
            self.conn.rollback()
            exit()

    def update_edw_table_medallero(self) -> None:
        """ Update Is_Active to Zero """
        table_name = 'edw_medallero'
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"UPDATE {self.schema}.{table_name} SET is_active = 0 WHERE is_active = 1;")
                self.conn.commit()
                print("update_edw_table_medallero() -> executed successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            print('Error: update_edw_table_medallero()')
            self.conn.rollback()
            exit()