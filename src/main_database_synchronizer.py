import traceback
import json
from sqlalchemy import create_engine

# pip install SQLAlchemy
# pip install PyMySQL


class DBConnector:
    def connect_to_db(self, db_driver, user, password, host, database):
        self.db_driver = db_driver
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.connection_string = f'{db_driver}://{user}:{password}@{host}/{database}'
        self.engine = create_engine(self.connection_string)
        self.connection = self.engine.connect()


    def execute_statement(self, statement):
        cursor = self.connection.execute(statement)
        query_result = cursor.fetchall()
        return query_result


    def get_table_structures(self):
        # get tables and views list
        query_result_tables = source_db.execute_statement(f'show full tables from {self.database};')
        tables = [row[0] for row in query_result_tables if row[1] == 'BASE TABLE']
        views = [row[0] for row in query_result_tables if row[1] == 'VIEW']

        # get tables structure
        self.table_structures = []
        for table in tables:
            query_result_columns = source_db.execute_statement(f'show full columns from {table};')
            columns = [(row[0], row[1]) for row in query_result_columns]
            self.table_structures.append((table, columns))

        # get views structure
        self.view_structures = []
        for view in views:
            query_result_columns = source_db.execute_statement(f'show full columns from {view};')
            columns = [(row[0], row[1]) for row in query_result_columns]
            self.view_structures.append((table, columns))


    def print_table_structures(self):
        print('--- table structures ---')
        for table in self.table_structures:
            print(f'table name: {table[0]}')
            for column in table[1]:
                print(f'- {column[0]} ({column[1]})')

        print('\n--- view structures ---')
        for view in self.view_structures:
            print(f'view name: {view[0]}')
            for column in view[1]:
                print(f'- {column[0]} ({column[1]})')


if __name__ == '__main__':
    try:
        with open('../util/database_login_data.json', 'r') as file:
            login_data = json.load(file)

        source_db = DBConnector()
        source_db.connect_to_db('mysql+pymysql', login_data['username'], login_data['password'], login_data['host'], 'gym')
        destination_db = DBConnector()
        destination_db.connect_to_db('mysql+pymysql', login_data['username'], login_data['password'], login_data['host'], 'test')

        source_db.get_table_structures()
        destination_db.get_table_structures()

        print('--- source DB structures ---')
        source_db.print_table_structures()
        print('\n--- destination DB structures ---')
        destination_db.print_table_structures()


    except Exception:
        traceback.print_exc()