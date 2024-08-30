import psycopg2

class DatabaseConection():
    def __init__(self, config: dict) -> None:
        self.config = config

    def get_conn(self):
        """ Connect to the Redshift database server """

        username = self.config.get('REDSHIFT_USERNAME')
        password = self.config.get('REDSHIFT_PASSWORD')
        host = self.config.get('REDSHIFT_HOST')
        port = self.config.get('REDSHIFT_PORT', '5439')
        dbname = self.config.get('REDSHIFT_DBNAME')

        try:
            # connecting to the Redshift server
            with psycopg2.connect(f"dbname={dbname} host={host} port={port} user={username} password={password}") as self.conn:
                print('Connected to the Redshift server.')
                return self.conn
        except (Exception, psycopg2.DatabaseError ) as error:
            print(error)
            print('Error: get_conn()')
            exit()

    def close_conn(self) -> None:
        """ Close connection """
        try:
            self.conn.close()
            print('Connection closed...')
        except (Exception, psycopg2.DatabaseError ) as error:
            print(error)
            print('Error: close_conn()')
            exit()


    def validate_table_exists(self, table_name) -> bool:
        with self.conn.cursor() as cursor:
            cursor.execute(f"""
                    SELECT 1 FROM information_schema.tables 
                    WHERE  table_schema = {self.schema}
                    AND    table_name   = '{table_name}';              
                """)
            table_exists = cursor.fetchone()
            if table_exists:
                return True
            else:
                return False
            
    def print_table_message(self, table_name: str, type: int) -> None:
        """ type 1: Success | type 2: Error"""
        if type == 1:
            print(f"{table_name} created successfully.")
        elif type == 2:
            print(f'Error creating: {table_name}')

    def get_last_execution(self) -> tuple:
        """ Get the last execution """
        table_name = 'stg_executionlog'
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"SELECT MAX(last_execution) FROM {self.schema}.{table_name};")
                last_execution = cursor.fetchone()
                print(f"get_last_execution() -> {str(last_execution[0])}")
                return last_execution[0]
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            print('Error: get_last_execution()')
            exit()