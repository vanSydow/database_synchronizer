import traceback
import json
import os
import glob
from time import sleep
from sqlalchemy import create_engine
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
from tkinter import filedialog


# pip install SQLAlchemy
# pip install PyMySQL

dev_util_dir = f'{os.getcwd()}\\..\\util'
prod_util_dir = f'{os.getcwd()}\\util'

maria_db = 'MariaDB'
mysql = 'MySQL'
postgres = 'Postgres'


class App():
    def __init__(self, root):
        root.title('Database Synchronizer')
        root.geometry('600x300')

        shared_data = SharedData()
        source_db_connector = DBConnector()
        destination_db_connector = DBConnector()
        profile_frame = ProfileFrame(root, shared_data)
        source_frame = DBConnectionFrame(root, shared_data, 'Source')
        destination_frame = DBConnectionFrame(root, shared_data, 'Destination')
        result_frame = ResultFrame(root, shared_data, destination_db_connector)
        compare_frame = CompareFrame(root, source_db_connector, destination_db_connector, source_frame, destination_frame, result_frame, shared_data)


class SharedData:
    def __init__(self):
        self.profile = {}
        self.new_tables = []
        self.deleted_tables = []
        self.altered_tables = []


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
            query_result = connection.execute(statement)
            try:
                result = query_result.fetchall()
            except Exception:
                print(f'{statement} does not return values.')
                return None
        return result

    # TODO: add other databases
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


class ProfileFrame:
    def __init__(self, root, shared_data):
        self.frame = tk.Frame(root, width=600, height=100)
        self.frame.pack(side='top', anchor='w')
        self.profile_header_label = tk.Label(self.frame, text=f'Profile selected:')
        self.profile_header_label.grid(row=0, column=0)
        self.profile_text = tk.StringVar()
        self.profile_text_label = tk.Label(self.frame, width=30, bg='white', textvariable=self.profile_text)
        self.profile_text_label.grid(row=0, column=1)
        self.open_button = ttk.Button(self.frame, text='Open a File', command=lambda:self.select_file(shared_data))
        self.open_button.grid(row=0, column=2)

        # load last modified file
        profile_list = glob.glob(f'{dev_util_dir}\\*.json')
        latest_profile = max(profile_list, key=os.path.getctime)
        with open(latest_profile, 'r') as file:
            shared_data.profile = json.load(file)
        self.profile_text.set(latest_profile.split('\\')[-1])

    def select_file(self, shared_data):
        filetypes = (('JSON files', '*.json'), ('All files', '*.*'))
        profile_filename = filedialog.askopenfilename(title='Open profile', initialdir=dev_util_dir, filetypes=filetypes)
        self.profile_text.set(profile_filename.split('/')[-1])
        with open(profile_filename, 'r') as file:
            shared_data.profile = json.load(file)


class DBConnectionFrame:
    def __init__(self, root, shared_data, src_dst):
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
        self.write_default_connection_values(shared_data, src_dst)

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
        self.password_input = ttk.Entry(self.frame, show='*')
        self.test_connection_button = ttk.Button(self.frame, text='Test connection', command=self.test_connection)

    def add_to_grid_connection_window(self, offset):
        self.header_label.grid(row=0, column=0+offset, columnspan=2)
        self.db_label.grid(row=1, column=0 + offset)
        self.db_dropdown.grid(row=1, column=1 + offset)
        self.host_label.grid(row=2, column=0 + offset)
        self.host_input.grid(row=2, column=1 + offset)
        self.database_name_label.grid(row=3, column=0 + offset)
        self.database_name_input.grid(row=3, column=1 + offset)
        self.user_label.grid(row=4, column=0 + offset)
        self.user_input.grid(row=4, column=1 + offset)
        self.password_label.grid(row=5, column=0 + offset)
        self.password_input.grid(row=5, column=1 + offset)
        self.test_connection_button.grid(row=6, column=0+offset, columnspan=2)

    def write_default_connection_values(self, shared_data, src_dst):
        try:
            # with open('../util/default_databases.json', 'r') as file:
            #     login_data = json.load(file)

            self.db_dropdown.set(shared_data.profile[src_dst]['db_type'])
            self.host_input.insert(0, shared_data.profile[src_dst]['host'])
            self.database_name_input.insert(0, shared_data.profile[src_dst]['db_name'])
            self.user_input.insert(0, shared_data.profile[src_dst]['username'])
            self.password_input.insert(0, shared_data.profile[src_dst]['password'])

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
    def __init__(self, root, source_db_connector, destination_db_connector, source_frame, destination_frame, result_frame, shared_data):
        self.frame = tk.Frame(root, width=600, height=100)
        self.frame.pack(side='bottom')
        self.compare_db_button = ttk.Button(self.frame, text='Compare Databases',
                                            command=lambda:self.compare_db(source_db_connector, destination_db_connector, source_frame, destination_frame, result_frame, shared_data))
        self.compare_db_button.grid(row=0, column=0)

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

    def compare_db(self, source_db_connector, destination_db_connector, source_frame, destination_frame, result_frame, shared_data):
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

        # self.print_table_structures(source_db_connector.database, self.source_table_structures, self.source_view_structures)
        # self.print_table_structures(destination_db_connector.database, self.destination_table_structures, self.destination_view_structures)

        # clear screen and set next screen
        source_frame.frame.destroy()
        destination_frame.frame.destroy()
        self.frame.destroy()
        result_frame.frame.pack(side='top')

        # table sync logic
        print('#######################################################################################################')
        print('### source ###')
        print('# tables')
        for elem in self.source_table_structures: print(elem)
        print('# views')
        for elem in self.source_view_structures: print(elem)

        print('\n### destination ###')
        print('-#tables')
        for elem in self.destination_table_structures: print(elem)
        print('# views')
        for elem in self.destination_view_structures: print(elem)

        source_table_list = [elem[0] for elem in self.source_table_structures]
        source_view_list = [elem[0] for elem in self.source_table_structures]
        destination_table_list = [elem[0] for elem in self.destination_table_structures]
        destination_view_list = [elem[0] for elem in self.destination_table_structures]
        equal_table_list = set(source_table_list) & set(destination_table_list)

        # TODO: check for equal columns (table has been renamed)
        print('\n### New tables ###')
        result_frame.result_text.insert(tk.END, '### New tables ###\n')
        for table_struct in self.source_table_structures:
            if table_struct[0] not in destination_table_list:
                shared_data.new_tables.append(table_struct)
                print(table_struct)
                result_frame.result_text.insert(tk.END, f'{table_struct[0]}\n')
                for column in table_struct[1]:
                    result_frame.result_text.insert(tk.END, f'- {column[0]} ({column[1]})\n')
                result_frame.result_text.insert(tk.END, '\n')

        # TODO: check for equal columns (table has been renamed)
        print('\n### Deleted tables ###')
        result_frame.result_text.insert(tk.END, '\n### Deleted tables ###\n')
        for table_struct in self.destination_table_structures:
            if table_struct[0] not in source_table_list:
                shared_data.deleted_tables.append(table_struct)
                print(table_struct)
                result_frame.result_text.insert(tk.END, f'{table_struct[0]}\n')
                for column in table_struct[1]:
                    result_frame.result_text.insert(tk.END, f'- {column[0]} ({column[1]})\n')
                result_frame.result_text.insert(tk.END, '\n')

        print('\n### Altered tables ###')
        result_frame.result_text.insert(tk.END, '\n### Altered tables ###\n')
        equal_source_tables = []
        equal_destination_tables = []

        # check for tables with same name
        for source_table_struct in self.source_table_structures:
            for destination_table_struct in self.destination_table_structures:
                if source_table_struct[0] == destination_table_struct[0]:
                    equal_source_tables.append(source_table_struct)
                    equal_destination_tables.append(destination_table_struct)

        # check for exclusive column names in source-destination table pair
        for i in range(len(equal_source_tables)):
            source_column_names = [column[0] for column in equal_source_tables[i][1]]
            destination_column_names = [column[0] for column in equal_destination_tables[i][1]]
            unique_source_columns = set(source_column_names) - set(destination_column_names)
            unique_destination_columns = set(destination_column_names) - set(source_column_names)

            print(2)


class ResultFrame:
    def __init__(self, root, shared_data, destination_db_connector):
        self.frame = tk.Frame(root, width=580, height=250, padx=10, pady=10)
        self.deploy_button = ttk.Button(self.frame, text='Deploy changes to database', command=lambda:self.deploy_to_database(shared_data, destination_db_connector))
        self.deploy_button.pack(side='bottom')
        self.generate_script_button = ttk.Button(self.frame, text='Generate script', command=lambda:self.ddl_script_to_file(shared_data))
        self.generate_script_button.pack(side='bottom')
        self.result_text = tk.Text(self.frame, height=200, width=600)
        self.result_text.pack(side='top')
        self.ddl_script = ''

    def generate_ddl_script(self, shared_data):
        # create table statements
        self.ddl_script = ''
        for table in shared_data.new_tables:
            self.ddl_script += f'CREATE TABLE IF NOT EXISTS {table[0]} (\n'
            for column in table[1]:
                self.ddl_script += f'    {column[0]} {column[1]},\n'
            self.ddl_script = self.ddl_script[:-2]
            self.ddl_script += '\n);\n\n'

        # delete table statements
        for table in shared_data.deleted_tables:
            self.ddl_script += f'DROP TABLE IF EXISTS {table[0]};\n\n'

        self.ddl_script = self.ddl_script.strip('\n')

    def ddl_script_to_file(self, shared_data):
        try:
            self.generate_ddl_script(shared_data)
            if not self.ddl_script.strip():
                messagebox.showinfo('DDL Script Generation', 'Nothing to create ;)')
            else:
                with open('../util/DDL_Script.sql', 'w') as file:
                    file.write(self.ddl_script)
                messagebox.showinfo('DDL Script Generation', 'DDL script generated sucessfully :)')

        except Exception:
            messagebox.showerror('DDL Script Generation', 'DDL script generation failed :(')
            traceback.print_exc()

    def deploy_to_database(self, shared_data, db_connector_destination):
        try:
            self.generate_ddl_script(shared_data)
            if not self.ddl_script.strip():
                messagebox.showinfo('DDL Script Deployment', 'Nothing to deploy ;)')
            else:
                mb_answer = messagebox.askquestion('Deploy changes', f'Deploy changes to {db_connector_destination.host}/{db_connector_destination.database} ?')
                if mb_answer == 'yes':
                    for statement in self.ddl_script.split(';'):    # need for multi statements
                        if statement: db_connector_destination.execute_statement(f'{statement};')
                    messagebox.showinfo('DDL Script Deployment', 'DDL script deployment sucessfully :)')

        except Exception:
            messagebox.showerror('DDl Script Deployment', 'DDL script deployment failed :(')
            traceback.print_exc()


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