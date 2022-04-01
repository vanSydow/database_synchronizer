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

        compare_frame = CompareFrame(root)
        source_frame = DBConnectionFrame(root, 'Source')
        destination_frame = DBConnectionFrame(root, 'Destination')


        # create a model
        # model = Model('hello@pythontutorial.net')
        #
        # # create a view and place it on the root window
        # view = View(self)
        # view.grid(row=0, column=0, padx=10, pady=10)
        #
        # # create a controller
        # controller = Controller(model, view)
        #
        # # set the controller to view
        # view.set_controller(controller)


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
        query_result_tables = self.execute_statement(f'show full tables from {self.database};')
        tables = [row[0] for row in query_result_tables if row[1] == 'BASE TABLE']
        views = [row[0] for row in query_result_tables if row[1] == 'VIEW']

        # get tables structure
        self.table_structures = []
        for table in tables:
            query_result_columns = self.execute_statement(f'show full columns from {table};')
            columns = [(row[0], row[1]) for row in query_result_columns]
            self.table_structures.append((table, columns))

        # get views structure
        self.view_structures = []
        for view in views:
            query_result_columns = self.execute_statement(f'show full columns from {view};')
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
        self.db_dropdown = ttk.Combobox(self.frame, values=[maria_db, mysql, postgres, sql_server], state='readonly')
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
            if self.db_dropdown.get() == maria_db:
                self.db_driver = 'mysql+pymysql'
            else:
                print('database driver invalid')

            engine = create_engine(
                f'{self.db_driver}://{self.user_input.get()}:{self.password_input.get()}@{self.host_input.get()}/{self.database_name_input.get()}',
                connect_args={'connect_timeout': 3}).connect()
            messagebox.showinfo('Connection Test', 'Connection test successful :)')

        except Exception:
            messagebox.showerror('Connection Test', 'Connection test failed :(')
            traceback.print_exc()


class CompareFrame:
    def __init__(self, root):
        self.frame = tk.Frame(root, width=600, height=100)
        self.frame.pack(side='bottom')
        self.compare_db_button = ttk.Button(self.frame, text='Compare Databases', command=self.compare_db)
        self.compare_db_button.grid(row=0, column=0)

    def compare_db(self):
        self.source_db = DBConnector()
        self.destination_db = DBConnector()

        # self.source_db.connect_to_db(source.db_driver, login_data['username'], login_data['password'],
        #                              login_data['host'], 'gym')


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

        # source_db = DBConnector()
        # source_db.connect_to_db('mysql+pymysql', login_data['username'], login_data['password'], login_data['host'], 'gym')
        # destination_db = DBConnector()
        # destination_db.connect_to_db('mysql+pymysql', login_data['username'], login_data['password'], login_data['host'], 'test')
        #
        # source_db.get_table_structures()
        # destination_db.get_table_structures()
        #
        # print('--- source DB structures ---')
        # source_db.print_table_structures()
        # print('\n--- destination DB structures ---')
        # destination_db.print_table_structures()


    except Exception:
        traceback.print_exc()