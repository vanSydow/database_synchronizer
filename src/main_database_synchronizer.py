import traceback
import json
import os
import glob
import pandas as pd
from time import sleep
from sqlalchemy import create_engine
import tkinter as tk
from tkinter import ttk, messagebox, filedialog


# pip install pandas
# pip install SQLAlchemy
# pip install PyMySQL
# pip install psycopg2

dev_util_dir = f'{os.getcwd()}\\..\\util'
dev_profiles_dir = f'{os.getcwd()}\\..\\profiles'
dev_ddl_scripts_dir = f'{os.getcwd()}\\..\\ddl_scripts'
prod_util_dir = f'{os.getcwd()}\\util'
prod_profiles_dir = f'{os.getcwd()}\\profiles'
prod_ddl_scripts_dir = f'{os.getcwd()}\\ddl_scripts'

window_width = 600
window_height = 300
maria_db = 'MariaDB'
mysql = 'MySQL'
postgres = 'PostgreSQL'


class App():
    def __init__(self, root):
        root.title('Database Synchronizer')
        root.geometry(f'{window_width}x{window_height}')

        # shared_data = SharedData()
        source_db_connector = DBConnector()
        destination_db_connector = DBConnector()
        profile_frame = ProfileFrame(root)
        source_frame = DBConnectionFrame(root, 'Source', profile_frame)
        destination_frame = DBConnectionFrame(root, 'Destination', profile_frame)
        compare_frame = CompareFrame(root, source_db_connector, destination_db_connector, profile_frame, source_frame, destination_frame)


class SharedData:
    profile = {}
    new_tables = []
    deleted_tables = []
    altered_tables = []
    structure_changes = []
    database_type = ''


class DBConnector:
    def __init__(self):
        self.table_structures = []
        self.view_structures = []

    def connect_to_db(self, db_driver, user, password, host, database):
        if db_driver == maria_db: self.db_driver = 'mysql+pymysql'
        elif db_driver == postgres: self.db_driver = 'postgresql'
        # TODO: add other database drivers
        SharedData.database_type = db_driver
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        try:
            if hasattr(self, 'engine'):
                self.engine.dispose()
            self.connection_string = f'{self.db_driver}://{self.user}:{self.password}@{self.host}/{self.database}'
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

    def execute_statement_pd(self, statement):
        with self.engine.connect() as connection:
            result = pd.read_sql(statement, connection)
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
    def __init__(self, root):
        self.frame = tk.Frame(root, width=window_width, height=50)
        self.frame.pack(side='top', anchor='w')
        self.profile_header_label = tk.Label(self.frame, text=f'Profile selected:')
        self.profile_header_label.grid(row=0, column=0)
        self.profile_text = tk.StringVar()
        self.profile_text_label = tk.Label(self.frame, width=30, bg='white', textvariable=self.profile_text)
        self.profile_text_label.grid(row=0, column=1)
        self.open_button = ttk.Button(self.frame, text='Open a File', command=self.select_file)
        self.open_button.grid(row=0, column=2)
        self.db_label = tk.Label(self.frame, text='Database type:')
        self.db_label.grid(row=1, column=0)
        self.db_dropdown = ttk.Combobox(self.frame, values=[maria_db, postgres], state='readonly')  # TODO: add other databases
        self.db_dropdown.grid(row=1, column=1)

        # load last modified file
        profile_list = glob.glob(f'{dev_profiles_dir}\\*.json')
        latest_profile = max(profile_list, key=os.path.getmtime)
        try:
            with open(latest_profile, 'r') as file:
                SharedData.profile = json.load(file)
            self.profile_text.set(latest_profile.split('\\')[-1])
        except Exception:
            messagebox.showerror('Error', 'Error opening most recent profile file.')
            traceback.print_exc()

    def select_file(self):
        filetypes = [('JSON files', '*.json')]
        profile_filename = filedialog.askopenfilename(title='Open profile', initialdir=dev_profiles_dir, filetypes=filetypes)
        try:
            with open(profile_filename, 'r') as file:
                SharedData.profile = json.load(file)
                self.profile_text.set(profile_filename.split('/')[-1])
        except Exception:
            messagebox.showerror('Error', 'Error opening profile file.')
            traceback.print_exc()

        for instance in DBConnectionFrame.instances:
            instance.write_default_connection_values(self.db_dropdown)


class DBConnectionFrame:
    instances = []

    def __init__(self, root, src_dst, profile_frame):
        self.__class__.instances.append(self)
        self.src_dst = src_dst
        if self.src_dst == 'Source':
            self.offset = 0
            self.side = 'left'
        else:
            self.offset = 2
            self.side = 'right'

        self.frame = tk.Frame(root, width=200, height=100)
        self.frame.pack(side=self.side)
        self.create_lables_connection_window()
        self.create_elements_connection_window()
        self.add_to_grid_connection_window(self.offset)
        self.write_default_connection_values(profile_frame.db_dropdown)

    def create_lables_connection_window(self):
        self.header_label = tk.Label(self.frame, text=f'{self.src_dst} database')
        self.db_label = tk.Label(self.frame, text='Database type:')
        self.host_label = tk.Label(self.frame, text='Database address:')
        self.database_name_label = tk.Label(self.frame, text='Database name:')
        self.user_label = tk.Label(self.frame, text='User name:')
        self.password_label = tk.Label(self.frame, text='Password:')

    def create_elements_connection_window(self):
        self.db_dropdown = ttk.Combobox(self.frame, values=[maria_db, postgres], state='readonly')    # TODO: add other databases
        self.host_input = ttk.Entry(self.frame)
        self.database_name_input = ttk.Entry(self.frame)
        self.user_input = ttk.Entry(self.frame)
        self.password_input = ttk.Entry(self.frame, show='*')
        self.test_connection_button = ttk.Button(self.frame, text='Test connection', command=self.test_connection)

    def add_to_grid_connection_window(self, offset):
        self.header_label.grid(row=0, column=0+offset, columnspan=2)
        # self.db_label.grid(row=1, column=0 + offset)
        # self.db_dropdown.grid(row=1, column=1 + offset)
        self.host_label.grid(row=2, column=0 + offset)
        self.host_input.grid(row=2, column=1 + offset)
        self.database_name_label.grid(row=3, column=0 + offset)
        self.database_name_input.grid(row=3, column=1 + offset)
        self.user_label.grid(row=4, column=0 + offset)
        self.user_input.grid(row=4, column=1 + offset)
        self.password_label.grid(row=5, column=0 + offset)
        self.password_input.grid(row=5, column=1 + offset)
        self.test_connection_button.grid(row=6, column=0+offset, columnspan=2)

    def write_default_connection_values(self, db_dropdown):
        try:
            self.db_dropdown.set(SharedData.profile['db_type'])
            db_dropdown.set(SharedData.profile['db_type'])
            self.host_input.delete(0, 'end')
            self.host_input.insert(0, SharedData.profile[self.src_dst]['host'])
            self.database_name_input.delete(0, 'end')
            self.database_name_input.insert(0, SharedData.profile[self.src_dst]['db_name'])
            self.user_input.delete(0, 'end')
            self.user_input.insert(0, SharedData.profile[self.src_dst]['username'])
            self.password_input.delete(0, 'end')
            self.password_input.insert(0, SharedData.profile[self.src_dst]['password'])

        except Exception:
            traceback.print_exc()

    def test_connection(self):
        try:
            if self.db_dropdown.get() == maria_db:
                self.db_driver = 'mysql+pymysql'
            elif self.db_dropdown.get() == postgres:
                self.db_driver = 'postgresql'
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
    def __init__(self, root, source_db_connector, destination_db_connector, profile_frame, source_frame, destination_frame):
        self.frame = tk.Frame(root, width=window_width, height=100)
        self.frame.pack(side='bottom')
        self.compare_db_button = ttk.Button(self.frame, text='Compare DBs', command=lambda:self.compare_db(source_db_connector, destination_db_connector, source_frame, destination_frame))
        # self.compare_db_button.grid(row=0, column=0)
        self.init_button = ttk.Button(self.frame, text='Initialize DBs', command=lambda: self.init_dbs(source_db_connector, profile_frame, source_frame, destination_frame))
        self.init_button.grid(row=1, column=0)
        self.source_structure_changes_button = ttk.Button(self.frame, text='Get source DB changes', command=lambda: self.source_structure_changes(source_db_connector, destination_db_connector, profile_frame, source_frame, destination_frame))
        self.source_structure_changes_button.grid(row=2, column=0)

    def init_dbs(self, source_db_connector, profile_frame, source_frame, destination_frame):
        mb_answer = messagebox.askquestion(
            'Initialize DBs',
            f'Would you like to create a new profile and initialize it with the given database credentials?\n'
            f'CAUTION: Make sure that both databases have the same structure during this process!'
        )
        if mb_answer == 'yes':
            file_output = {
                'db_type': profile_frame.db_dropdown.get(),
                source_frame.src_dst : {
                    # 'db_type': source_frame.db_dropdown.get(),
                    'host': source_frame.host_input.get(),
                    'db_name': source_frame.database_name_input.get(),
                    'username': source_frame.user_input.get(),
                    'password': source_frame.password_input.get()
                },
                destination_frame.src_dst: {
                    # 'db_type': destination_frame.db_dropdown.get(),
                    'host': destination_frame.host_input.get(),
                    'db_name': destination_frame.database_name_input.get(),
                    'username': destination_frame.user_input.get(),
                    'password': destination_frame.password_input.get()
                }
            }

            source_db_connector.connect_to_db(source_frame.db_dropdown.get(),
                                            source_frame.user_input.get(),
                                            source_frame.password_input.get(),
                                            source_frame.host_input.get(),
                                            source_frame.database_name_input.get())

            query_result = source_db_connector.execute_statement_pd(
                f"select pn.oid as schema_id, pn.nspname as schema_name, pc.oid as table_id, pc.relname as table_name, pa.attname as column_name, pa.atttypid as data_type_id, pt.typname as data_type_name "
                f"from pg_catalog.pg_namespace pn "
                f"join pg_catalog.pg_class pc on pn.oid = pc.relnamespace "
                f"join pg_catalog.pg_attribute pa on pc.oid = pa.attrelid "
                f"join pg_catalog.pg_type pt on pa.atttypid = pt.oid "
                f"where "
                f"    pn.nspname not like 'pg_%%' and pn.nspname != 'information_schema' and "
                f"    attname not in ('cmax', 'cmin', 'ctid', 'tableoid', 'xmax', 'xmin') "
                f"order by pn.oid, pc.oid;"
            )

            if query_result.empty:
                messagebox.showerror('Error', 'Source database is empty.')
                return -1

            file_output['source_struct'] = query_result.to_dict()
            json_output = json.dumps(file_output, indent=4)
            filetypes = [('JSON files', '*.json')]
            with filedialog.asksaveasfile(title='Save profile', initialdir=dev_profiles_dir, filetypes=filetypes, defaultextension='.json') as file:
                file.write(json_output)

    def source_structure_changes(self, source_db_connector, destination_db_connector, profile_frame, source_frame, destination_frame):
        # get current source table structure
        source_db_connector.connect_to_db(source_frame.db_dropdown.get(),
                                          source_frame.user_input.get(),
                                          source_frame.password_input.get(),
                                          source_frame.host_input.get(),
                                          source_frame.database_name_input.get())

        new_source_struct = source_db_connector.execute_statement_pd(
            f"select pn.oid as schema_id, pn.nspname as schema_name, pc.oid as table_id, pc.relname as table_name, pa.attname as column_name, pa.atttypid as data_type_id, pt.typname as data_type_name "
            f"from pg_catalog.pg_namespace pn "
            f"join pg_catalog.pg_class pc on pn.oid = pc.relnamespace "
            f"join pg_catalog.pg_attribute pa on pc.oid = pa.attrelid "
            f"join pg_catalog.pg_type pt on pa.atttypid = pt.oid "
            f"where "
            f"    pn.nspname not like 'pg_%%' and pn.nspname != 'information_schema' and "
            f"    attname not in ('cmax', 'cmin', 'ctid', 'tableoid', 'xmax', 'xmin') "
            f"order by pn.oid, pc.oid;"
        )

        if new_source_struct.empty:
            messagebox.showerror('Error', 'Source database is empty.')
            return -1

        # open old source table structure
        with open(f'{dev_profiles_dir}\\{profile_frame.profile_text.get()}') as file:
            json_input = json.load(file)
        old_source_struct = pd.DataFrame.from_dict(json_input['source_struct'])

        # TODO: speed optimization
        SharedData.structure_changes = []    # old_schema_name, new_schema_name, old_table_name, new_table_name, old_colun_name, new_column_name, old_data_type_name, new_data_type_name, cdc (I, U, D and S, T, C, D)

        # check if new schemas have been added
        old_schema_ids = old_source_struct.schema_id.unique()
        new_schema_ids_only = pd.merge(old_source_struct['schema_id'], new_source_struct['schema_id'], on='schema_id', how='right', indicator=True).query('_merge=="right_only"')
        new_schema_ids_only = new_schema_ids_only.schema_id.unique()

        # add new schemas
        for new_schema_id in new_schema_ids_only:
            new_schema_filtered = new_source_struct.loc[new_source_struct['schema_id'] == new_schema_id]
            SharedData.structure_changes.append([
                None, new_schema_filtered.iloc[0]['schema_name'],
                None, None, None, None, None, None, 'IS'
            ])

            # add new tables
            new_table_ids = new_schema_filtered.table_id.unique()
            for new_table_id in new_table_ids:
                new_table_filtered = new_schema_filtered[new_schema_filtered['table_id'] == new_table_id]
                SharedData.structure_changes.append([
                    None, new_schema_filtered.iloc[0]['schema_name'],
                    None, new_table_filtered.iloc[0]['table_name'],
                    None, None, None, None, 'IT'
                ])

                # add new columns
                for column in new_table_filtered.itertuples(index=False):
                    SharedData.structure_changes.append([
                        None, new_schema_filtered.iloc[0]['schema_name'],
                        None, new_table_filtered.iloc[0]['table_name'],
                        None, column.column_name,
                        None, column.data_type_name,
                        'IC'
                    ])

        for old_schema_id in old_schema_ids:
            old_schema_filtered = old_source_struct.loc[old_source_struct['schema_id'] == old_schema_id]

            # check if old schemas have been deleted
            if old_schema_id not in new_source_struct['schema_id'].to_numpy():
                SharedData.structure_changes.append([
                    old_schema_filtered.iloc[0]['schema_name'], None,
                    None, None, None, None, None, None, 'DS'
                ])

            # old schema still exists
            else:
                new_schema_filtered = new_source_struct.loc[new_source_struct['schema_id'] == old_schema_id]

                # check if schemas have been renamed
                if old_schema_filtered.iloc[0]['schema_name'] != new_schema_filtered.iloc[0]['schema_name']:
                    SharedData.structure_changes.append([
                        old_schema_filtered.iloc[0]['schema_name'], new_schema_filtered.iloc[0]['schema_name'],
                        None, None, None, None, None, None, 'US'
                    ])

                # check if new tables have been added
                old_table_ids = old_schema_filtered.table_id.unique()
                new_table_ids_only = pd.merge(old_schema_filtered['table_id'], new_schema_filtered['table_id'], on='table_id', how='right', indicator=True).query('_merge=="right_only"')
                new_table_ids_only = new_table_ids_only.table_id.unique()

                # add new tables
                for new_table_id in new_table_ids_only:
                    new_table_filtered = new_schema_filtered[new_schema_filtered['table_id'] == new_table_id]
                    SharedData.structure_changes.append([
                        None, new_schema_filtered.iloc[0]['schema_name'],
                        None, new_table_filtered.iloc[0]['table_name'],
                        None, None, None, None, 'IT'
                    ])

                    # add new columns
                    for new_column in new_table_filtered.itertuples(index=False):
                        SharedData.structure_changes.append([
                            None, new_schema_filtered.iloc[0]['schema_name'],
                            None, new_table_filtered.iloc[0]['table_name'],
                            None, new_column.column_name,
                            None, new_column.data_type_name,
                            'IC'
                        ])


                for old_table_id in old_table_ids:
                    old_table_filtered = old_schema_filtered[old_schema_filtered['table_id'] == old_table_id]

                    # check if old tables have been deleted
                    if old_table_id not in new_schema_filtered['table_id'].to_numpy():
                        SharedData.structure_changes.append([
                            old_schema_filtered.iloc[0]['schema_name'], new_schema_filtered.iloc[0]['schema_name'],
                            old_table_filtered.iloc[0]['table_name'], None,
                            None, None, None, None, 'DT'
                        ])

                    # old table still exists
                    else:
                        new_table_filtered = new_schema_filtered[new_schema_filtered['table_id'] == old_table_id]

                        # check if table has been renamed
                        if old_table_filtered.iloc[0]['table_name'] != new_table_filtered.iloc[0]['table_name']:
                            SharedData.structure_changes.append([
                                old_schema_filtered.iloc[0]['schema_name'], new_schema_filtered.iloc[0]['schema_name'],
                                old_table_filtered.iloc[0]['table_name'], new_table_filtered.iloc[0]['table_name'],
                                None, None, None, None, 'UT'
                            ])

                        for new_column in new_table_filtered.itertuples(index=False):
                            # insert new (or renamed) column - not sure because of lack of column id
                            if new_column.column_name not in old_table_filtered['column_name'].to_numpy():
                                SharedData.structure_changes.append([
                                    old_schema_filtered.iloc[0]['schema_name'], new_schema_filtered.iloc[0]['schema_name'],
                                    old_table_filtered.iloc[0]['table_name'], new_table_filtered.iloc[0]['table_name'],
                                    None, new_column.column_name,
                                    None, new_column.data_type_name,
                                    'IC'
                                ])

                            # check if data type of existing column has changed
                            else:
                                old_column = old_table_filtered[old_table_filtered['column_name'] == new_column.column_name]
                                if new_column.data_type_name != old_column.iloc[0]['data_type_name']:
                                    SharedData.structure_changes.append([
                                        old_schema_filtered.iloc[0]['schema_name'], new_schema_filtered.iloc[0]['schema_name'],
                                        old_table_filtered.iloc[0]['table_name'], new_table_filtered.iloc[0]['table_name'],
                                        old_column.iloc[0]['column_name'], new_column.column_name,
                                        old_column.iloc[0]['data_type_name'], new_column.data_type_name,
                                        'UD'
                                    ])

        result_window = tk.Toplevel()
        result_window.grab_set()
        result_frame = ResultFrame(result_window, destination_db_connector, destination_frame)
        result_frame.frame.pack()

    # def print_table_structures(self, database_name, table_structures, view_structures):
    #     print(f'\n### {database_name} ###')
    #     print('--- table structures ---')
    #     for table in table_structures:
    #         print(f'table name: {table[0]}')
    #         for column in table[1]:
    #             print(f'- {column[0]} ({column[1]})')
    #
    #     print('\n--- view structures ---')
    #     for view in view_structures:
    #         print(f'view name: {view[0]}')
    #         for column in view[1]:
    #             print(f'- {column[0]} ({column[1]})')

    # TODO
    # def compare_db(self, source_db_connector, destination_db_connector, source_frame, destination_frame, result_frame, shared_data):
    #     source_db_connector.connect_to_db(source_frame.db_dropdown.get(),
    #                                  source_frame.user_input.get(),
    #                                  source_frame.password_input.get(),
    #                                  source_frame.host_input.get(),
    #                                  source_frame.database_name_input.get())
    #     destination_db_connector.connect_to_db(destination_frame.db_dropdown.get(),
    #                                       destination_frame.user_input.get(),
    #                                       destination_frame.password_input.get(),
    #                                       destination_frame.host_input.get(),
    #                                       destination_frame.database_name_input.get())
    #
    #     source_db_connector.get_table_structures()
    #     destination_db_connector.get_table_structures()
    #
    #     self.source_table_structures = source_db_connector.table_structures
    #     self.source_view_structures = source_db_connector.view_structures
    #     self.destination_table_structures = destination_db_connector.table_structures
    #     self.destination_view_structures = destination_db_connector.view_structures
    #
    #     # self.print_table_structures(source_db_connector.database, self.source_table_structures, self.source_view_structures)
    #     # self.print_table_structures(destination_db_connector.database, self.destination_table_structures, self.destination_view_structures)
    #
    #     # clear screen and set next screen
    #     source_frame.frame.destroy()
    #     destination_frame.frame.destroy()
    #     self.frame.destroy()
    #     result_frame.frame.pack(side='top')
    #
    #     # table sync logic
    #     print('#######################################################################################################')
    #     print('### source ###')
    #     print('# tables')
    #     for elem in self.source_table_structures: print(elem)
    #     print('# views')
    #     for elem in self.source_view_structures: print(elem)
    #
    #     print('\n### destination ###')
    #     print('-#tables')
    #     for elem in self.destination_table_structures: print(elem)
    #     print('# views')
    #     for elem in self.destination_view_structures: print(elem)
    #
    #     source_table_list = [elem[0] for elem in self.source_table_structures]
    #     source_view_list = [elem[0] for elem in self.source_table_structures]
    #     destination_table_list = [elem[0] for elem in self.destination_table_structures]
    #     destination_view_list = [elem[0] for elem in self.destination_table_structures]
    #     equal_table_list = set(source_table_list) & set(destination_table_list)
    #
    #     # TODO: check for equal columns (table has been renamed)
    #     print('\n### New tables ###')
    #     result_frame.result_text.insert(tk.END, '### New tables ###\n')
    #     for table_struct in self.source_table_structures:
    #         if table_struct[0] not in destination_table_list:
    #             shared_data.new_tables.append(table_struct)
    #             print(table_struct)
    #             result_frame.result_text.insert(tk.END, f'{table_struct[0]}\n')
    #             for column in table_struct[1]:
    #                 result_frame.result_text.insert(tk.END, f'- {column[0]} ({column[1]})\n')
    #             result_frame.result_text.insert(tk.END, '\n')
    #
    #     # TODO: check for equal columns (table has been renamed)
    #     print('\n### Deleted tables ###')
    #     result_frame.result_text.insert(tk.END, '\n### Deleted tables ###\n')
    #     for table_struct in self.destination_table_structures:
    #         if table_struct[0] not in source_table_list:
    #             shared_data.deleted_tables.append(table_struct)
    #             print(table_struct)
    #             result_frame.result_text.insert(tk.END, f'{table_struct[0]}\n')
    #             for column in table_struct[1]:
    #                 result_frame.result_text.insert(tk.END, f'- {column[0]} ({column[1]})\n')
    #             result_frame.result_text.insert(tk.END, '\n')
    #
    #     print('\n### Altered tables ###')
    #     result_frame.result_text.insert(tk.END, '\n### Altered tables ###\n')
    #     equal_source_tables = []
    #     equal_destination_tables = []
    #
    #     # check for tables with same name
    #     for source_table_struct in self.source_table_structures:
    #         for destination_table_struct in self.destination_table_structures:
    #             if source_table_struct[0] == destination_table_struct[0]:
    #                 equal_source_tables.append(source_table_struct)
    #                 equal_destination_tables.append(destination_table_struct)
    #
    #     # check for exclusive column names in source-destination table pair
    #     for i in range(len(equal_source_tables)):
    #         source_column_names = [column[0] for column in equal_source_tables[i][1]]
    #         destination_column_names = [column[0] for column in equal_destination_tables[i][1]]
    #         unique_source_columns = set(source_column_names) - set(destination_column_names)
    #         unique_destination_columns = set(destination_column_names) - set(source_column_names)


class ResultFrame:
    def __init__(self, root, destination_db_connector, destination_frame):
        self.frame = tk.Frame(root, padx=10, pady=10)
        self.result_text = tk.Text(self.frame, width=100, height=40)   # width in letters and height in text lines
        self.result_text.pack()
        self.generate_script_button = ttk.Button(self.frame, text='Generate script', command=lambda:self.ddl_script_to_file())
        self.generate_script_button.pack()
        self.deploy_button = ttk.Button(self.frame, text='Deploy changes to database')
        self.deploy_button.pack()
        self.ddl_script = ''
        self.frame.bind('<Map>', lambda event: self.generate_ddl_script(destination_db_connector, destination_frame), add='+')
        self.frame.bind('<Map>', lambda event: self.show_structure_changes(), add='+')

    def generate_ddl_script(self, destination_db_connector, destination_frame):
        destination_db_connector.connect_to_db(destination_frame.db_dropdown.get(),
                                               destination_frame.user_input.get(),
                                               destination_frame.password_input.get(),
                                               destination_frame.host_input.get(),
                                               destination_frame.database_name_input.get())

        self.deploy_button.configure(command=lambda:self.deploy_to_database(destination_db_connector))
        if SharedData.structure_changes:
            self.ddl_script = ''
            if SharedData.database_type == postgres:
                for row in SharedData.structure_changes:
                    print(row)
                    if row[8] == 'IS':
                        self.ddl_script += f'CREATE SCHEMA {row[1]};\n\n'
                    elif row[8] == 'IT':
                        self.ddl_script += f'CREATE TABLE {row[1]}.{row[3]}();\n\n'
                    elif row[8] == 'IC' or row[8] == 'UC':
                        self.ddl_script += f'ALTER TABLE {row[1]}.{row[3]}\n' \
                                      f'ADD {row[5]} {row[7]};\n\n'
                    elif row[8] == 'US':
                        self.ddl_script += f'ALTER SCHEMA {row[0]}\n' \
                                      f'RENAME TO {row[1]};\n\n'
                    elif row[8] == 'UT':
                        self.ddl_script += f'ALTER TABLE {row[0]}.{row[2]}\n' \
                                      f'RENAME TO {row[3]};\n\n'
                    elif row[8] == 'UD':
                        self.ddl_script += f'ALTER TABLE {row[1]}.{row[3]}\n' \
                                      f'ALTER COLUMN {row[5]} TYPE {row[7]};\n\n'
                    elif row[8] == 'DS':
                        self.ddl_script += f'DROP SCHEMA IF EXISTS {row[0]};\n\n'
                    elif row[8] == 'DT':
                        self.ddl_script += f'DROP TABLE IF EXISTS {row[2]};\n\n'
                    else:
                        self.ddl_script += f'-- unhandled action for {row}\n\n'
            self.ddl_script = self.ddl_script[:-2]      # delete last linebreaks
            print(self.ddl_script)

        else:
            print('No changes')

    def show_structure_changes(self):
        if SharedData.structure_changes:
            # configure tags for text styling
            self.result_text.tag_config('headline', font=(None, 9, 'bold'))
            self.result_text.tag_config('new', foreground='green')
            self.result_text.tag_config('altered', foreground='blue')
            self.result_text.tag_config('deleted', foreground='red')

            self.result_text.insert(tk.END, '### Schema changes ###\n', 'headline')
            for row in SharedData.structure_changes:
                if row[8] == 'IS': self.result_text.insert(tk.END, f'- NEW schema "{row[1]}"\n', 'new')
                elif row[8] == 'US': self.result_text.insert(tk.END, f'- RENAMED schema "{row[0]}" to "{row[1]}"\n', 'altered')
                elif row[8] == 'DS': self.result_text.insert(tk.END, f'- DELETED schema "{row[0]}"\n', 'deleted')

            self.result_text.insert(tk.END, '\n### Table changes ###\n', 'headline')
            for row in SharedData.structure_changes:
                if row[8] == 'IT': self.result_text.insert(tk.END, f'- NEW table "{row[1]}.{row[3]}"\n', 'new')
                elif row[8] == 'UT': self.result_text.insert(tk.END, f'- RENAMED table "{row[1]}.{row[2]}" to "{row[1]}.{row[3]}"\n', 'altered')
                elif row[8] == 'DT': self.result_text.insert(tk.END, f'- DELETED table "{row[0]}.{row[2]}"\n', 'deleted')

            self.result_text.insert(tk.END, '\n### Column changes ###\n', 'headline')
            for row in SharedData.structure_changes:
                if row[8] == 'IC' or row[8] == 'UC': self.result_text.insert(tk.END, f'- NEW or RENAMED column "{row[1]}.{row[3]}.{row[5]} ({row[7]})"\n', 'new')
                elif row[8] == 'UD': self.result_text.insert(tk.END, f'- CHANGED column "{row[1]}.{row[3]}.{row[4]}" data type from "{row[6]}" to "{row[7]}"\n', 'altered')

        else:
            self.result_text.insert(tk.END, '### No changes ###')

    def ddl_script_to_file(self):
        try:
            if not self.ddl_script.strip():
                messagebox.showinfo('DDL Script Generation', 'Nothing to create')
            else:
                filetypes = [('SQL files', '*.sql')]
                with filedialog.asksaveasfile(title='Save profile', initialdir=dev_profiles_dir, filetypes=filetypes, defaultextension='.json') as file:
                    file.write(self.ddl_script)
                # messagebox.showinfo('DDL Script Generation', 'DDL script generated sucessfully :)')

        except Exception:
            messagebox.showerror('DDL Script Generation', 'DDL script generation failed')
            traceback.print_exc()

    def deploy_to_database(self, destination_db_connector):
        try:
            if not self.ddl_script.strip():
                messagebox.showinfo('DDL Script Deployment', 'Nothing to deploy')
            else:
                mb_answer = messagebox.askquestion('Deploy structure changes', f'Deploy structure changes to {destination_db_connector.host}/{destination_db_connector.database} ?')
                if mb_answer == 'yes':
                    for statement in self.ddl_script.split(';'):    # needed for multi statements
                        if statement: destination_db_connector.execute_statement(f'{statement};')
                    messagebox.showinfo('DDL Script Deployment', 'DDL script deployment sucessfully')

        except Exception:
            messagebox.showerror('DDl Script Deployment', 'DDL script deployment failed')
            traceback.print_exc()


if __name__ == '__main__':
    try:
        # with open(f'{dev_profiles_dir}\\test3.json', 'r') as file:
        #     json_input = json.load(file)

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