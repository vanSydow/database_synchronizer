import traceback
import json
from time import sleep
from sqlalchemy import create_engine
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox


# pip install SQLAlchemy
# pip install PyMySQL


maria_db = 'MariaDB'
mysql = 'MySQL'
postgres = 'Postgres'
sql_server = 'SQL Server'


class App():
    def __init__(self, root):
        root.title('Database Synchronizer')
        root.geometry('600x300')

        source_db_connector = DBConnector()
        destination_db_connector = DBConnector()
        source_frame = DBConnectionFrame(root, 'Source')
        destination_frame = DBConnectionFrame(root, 'Destination')
        compare_frame = CompareFrame(root, source_db_connector, destination_db_connector, source_frame, destination_frame)


class DBConnector:
    def __init__(self):
        self.table_structures = []
        self.view_structures = []

    def connect_to_db(self, db_driver, user, password, host, database):
        self.db_driver = db_driver
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        try:
            self.connection_string = f'{db_driver}://{user}:{password}@{host}/{database}'
            self.engine = create_engine(self.connection_string)

        except Exception:
            messagebox.showerror('Database Connection Error', 'Connecting to database failed :(')
            traceback.print_exc()

    def execute_statement(self, statement):
        with self.engine.connect() as connection:
            query_result = connection.execute(statement).fetchall()
        return query_result

    def get_table_structures(self):
        # get tables and views list
        query_result_tables = self.execute_statement(f'show full tables from {self.database};')
        tables = [row[0] for row in query_result_tables if row[1] == 'BASE TABLE']
        views = [row[0] for row in query_result_tables if row[1] == 'VIEW']

        # get tables structure
        for table in tables:
            query_result_columns = self.execute_statement(f'show full columns from {table};')
            columns = [(row[0], row[1]) for row in query_result_columns]
            self.table_structures.append((table, columns))

        # get views structure
        for view in views:
            query_result_columns = self.execute_statement(f'show full columns from {view};')
            columns = [(row[0], row[1]) for row in query_result_columns]
            self.view_structures.append((table, columns))

    # def print_table_structures(self):
    #     print(f'\n### {self.database} ###')
    #     print('--- table structures ---')
    #     for table in self.table_structures:
    #         print(f'table name: {table[0]}')
    #         for column in table[1]:
    #             print(f'- {column[0]} ({column[1]})')
    #
    #     print('\n--- view structures ---')
    #     for view in self.view_structures:
    #         print(f'view name: {view[0]}')
    #         for column in view[1]:
    #             print(f'- {column[0]} ({column[1]})')


class DBConnectionFrame:
    def __init__(self, root, src_dst):
        if src_dst == 'Source':
            self.offset = 0
            self.side = 'left'
        else:
            self.offset = 2
            self.side = 'right'

        self.frame = tk.Frame(root, width=200, height=100)
        self.frame.pack(side=self.side)
        self.create_lables_connection_window(src_dst)
        self.create_elements_connection_window()
        self.add_to_grid_connection_window(self.offset)
        self.write_default_connection_values(src_dst)

    def create_lables_connection_window(self, src_dst):
        self.header_label = tk.Label(self.frame, text=f'{src_dst} database')
        self.db_label = tk.Label(self.frame, text='Database type:')
        self.host_label = tk.Label(self.frame, text='Database address:')
        self.database_name_label = tk.Label(self.frame, text='Database name:')
        self.user_label = tk.Label(self.frame, text='User name:')
        self.password_label = tk.Label(self.frame, text='Password:')

    def create_elements_connection_window(self):
        self.db_dropdown = ttk.Combobox(self.frame, values=[maria_db], state='readonly')    # TODO: add other databases
        self.host_input = ttk.Entry(self.frame)
        self.database_name_input = ttk.Entry(self.frame)
        self.user_input = ttk.Entry(self.frame)
        self.password_input = ttk.Entry(self.frame)
        self.test_connection_button = ttk.Button(self.frame, text='Test connection', command=self.test_connection)

    def add_to_grid_connection_window(self, offset):
        self.db_label.grid(row=0, column=0 + offset)
        self.db_dropdown.grid(row=0, column=1 + offset)
        self.host_label.grid(row=1, column=0 + offset)
        self.host_input.grid(row=1, column=1 + offset)
        self.database_name_label.grid(row=2, column=0 + offset)
        self.database_name_input.grid(row=2, column=1 + offset)
        self.user_label.grid(row=3, column=0 + offset)
        self.user_input.grid(row=3, column=1 + offset)
        self.password_label.grid(row=4, column=0 + offset)
        self.password_input.grid(row=4, column=1 + offset)
        self.test_connection_button.grid(row=5, column=0 + offset, columnspan=2)

    def write_default_connection_values(self, src_dst):
        try:
            with open('../util/default_databases.json', 'r') as file:
                login_data = json.load(file)

            self.db_dropdown.set(login_data[src_dst]['db_type'])
            self.host_input.insert(0, login_data[src_dst]['host'])
            self.database_name_input.insert(0, login_data[src_dst]['db_name'])
            self.user_input.insert(0, login_data[src_dst]['username'])
            self.password_input.insert(0, login_data[src_dst]['password'])

        except Exception:
            traceback.print_exc()

    def test_connection(self):
        try:
            if self.db_dropdown.get() == maria_db: self.db_driver = 'mysql+pymysql'
            # TODO: add other database drivers

            engine = create_engine(
                f'{self.db_driver}://{self.user_input.get()}:{self.password_input.get()}@{self.host_input.get()}/{self.database_name_input.get()}',
                connect_args={'connect_timeout': 3}).connect()
            engine.close()
            messagebox.showinfo('Connection Test', 'Connection test successful :)')

        except Exception:
            messagebox.showerror('Connection Test', 'Connection test failed :(')
            traceback.print_exc()


class CompareFrame:
    def __init__(self, root, source_db_connector, destination_db_connector, source_frame, destination_frame):
        self.frame = tk.Frame(root, width=600, height=100)
        self.frame.pack(side='bottom')
        self.compare_db_button = ttk.Button(self.frame, text='Compare Databases',
                                            command=lambda:self.compare_db(source_db_connector, destination_db_connector, source_frame, destination_frame))
        self.compare_db_button.grid(row=0, column=0)

    def compare_db(self, source_db_connector, destination_db_connector, source_frame, destination_frame):
        if source_frame.db_dropdown.get() == maria_db: self.source_db_driver = 'mysql+pymysql'
        # TODO: add other database drivers
        if destination_frame.db_dropdown.get() == maria_db: self.destination_db_driver = 'mysql+pymysql'
        # TODO: add other database drivers

        source_db_connector.connect_to_db(self.source_db_driver,
                                     source_frame.user_input.get(),
                                     source_frame.password_input.get(),
                                     source_frame.host_input.get(),
                                     source_frame.database_name_input.get())
        destination_db_connector.connect_to_db(self.destination_db_driver,
                                          destination_frame.user_input.get(),
                                          destination_frame.password_input.get(),
                                          destination_frame.host_input.get(),
                                          destination_frame.database_name_input.get())

        source_db_connector.get_table_structures()
        destination_db_connector.get_table_structures()

        self.source_table_structures = source_db_connector.table_structures
        self.source_view_structures = source_db_connector.view_structures
        self.destination_table_structures = destination_db_connector.table_structures
        self.destination_view_structures = destination_db_connector.view_structures
        
        self.print_table_structures(source_db_connector.database, self.source_table_structures, self.source_view_structures)
        self.print_table_structures(destination_db_connector.database, self.destination_table_structures, self.destination_view_structures)

    def print_table_structures(self, database_name, table_structures, view_structures):
        print(f'\n### {database_name} ###')
        print('--- table structures ---')
        for table in table_structures:
            print(f'table name: {table[0]}')
            for column in table[1]:
                print(f'- {column[0]} ({column[1]})')

        print('\n--- view structures ---')
        for view in view_structures:
            print(f'view name: {view[0]}')
            for column in view[1]:
                print(f'- {column[0]} ({column[1]})')


if __name__ == '__main__':
    try:
        root = tk.Tk()
        app = App(root)

        # frame = tk.Frame(root, bg='cyan', width=200, height=200)
        # frame2 = tk.Frame(root, bg='cyan', width=200, height=200)
        # frame.grid(row=1, column=0)
        # frame2.grid(row=1, column=1)
        # header_label = tk.Label(frame2, text='database')
        # header_label.grid(row=1, columnspan=3)

        root.mainloop()

        # with open('../util/default_databases.json', 'r') as file:
        #     login_data = json.load(file)
        #
        # source_db = DBConnector()
        # source_db.connect_to_db('mysql+pymysql', login_data['Source']['username'], login_data['Source']['password'], login_data['Source']['host'], login_data['Source']['db_name'])
        # destination_db = DBConnector()
        # destination_db.connect_to_db('mysql+pymysql', login_data['Destination']['username'], login_data['Destination']['password'], login_data['Destination']['host'], login_data['Destination']['db_name'])
        #
        # source_db.get_table_structures()
        # destination_db.get_table_structures()
        #
        # source_db.print_table_structures()
        # destination_db.print_table_structures()


    except Exception:
        traceback.print_exc()