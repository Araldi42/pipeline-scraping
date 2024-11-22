import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

class Database:
    def __init__(self):
        """
        Initializes a new instance of the Database class.
        """
        self.dbname = os.getenv('DB_NAME')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.host = os.getenv('DB_HOST')
        self.port = os.getenv('DB_PORT')
        self.conn = None

    def __connect(self):
        """
        Establishes a connection to the database.
        """
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
        except psycopg2.Error as e:
            logging.error(e)

    def __disconnect(self):
        """
        Closes the connection to the database.
        """
        try:
            if self.conn:
                self.conn.close()
        except psycopg2.Error as e:
            logging.error(e)

    def execute_query_to_dataframe(self, query, params=None):
        """
        Executes a query on the database and returns the results as a pandas DataFrame.

        :param query: SQL query to be executed.
        :type query: str
        :param params: Parameters for the SQL query.
        :type params: tuple or None
        :return: Results of the query.
        :rtype: pandas.DataFrame
        """
        try:
            self.__connect()
            cur = self.conn.cursor()
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            result = cur.fetchall()
            columns_name = [desc[0] for desc in cur.description]
            df = pd.DataFrame(result, columns=columns_name)
            cur.close()
            self.__disconnect()
            return df
        except psycopg2.Error as e:
            logging.error(e)
            return None

    def execute_query(self, query, params=None):
        """
        Executes a query on the database and returns the results.

        :param query: SQL query to be executed.
        :type query: str
        :param params: Parameters for the SQL query.
        :type params: tuple or None
        :return: Results of the query.
        :rtype: list of tuples
        """
        try:
            self.__connect()
            cur = self.conn.cursor()
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            result = cur.fetchall()
            cur.close()
            self.__disconnect()
            return result
        except psycopg2.Error as e:
            logging.error(e)
            return None

    def execute_update(self, query, params=None):
        """
        Executes an update (INSERT, UPDATE, DELETE) on the database.

        :param query: SQL query to be executed.
        :type query: str
        :param params: Parameters for the SQL query.
        :type params: tuple or None
        """
        try:
            self.__connect()
            cur = self.conn.cursor()
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            self.conn.commit()
            cur.close()
            self.__disconnect()
        except psycopg2.Error as e:
            logging.error(e)

    def insert(self, table, columns, values):
        """
        Inserts a new row into a table.

        :param table: Name of the table.
        :type table: str
        :param columns: List of columns in the table.
        :type columns: list of str
        :param values: List of values to be inserted.
        :type values: list
        """
        try:
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in values])})"
            self.execute_update(query, values)
        except psycopg2.Error as e:
           logging.error(e)

    def read(self, table, columns=None, condition=None):
        """
        Reads data from a table.

        :param table: Name of the table.
        :type table: str
        :param columns: List of columns to be read (if None, reads all columns).
        :type columns: list of str or None
        :param condition: Condition to filter the results (optional).
        :type condition: str or None
        :return: Results of the query.
        :rtype: list of tuples
        """
        try:
            query = f"SELECT {', '.join(columns) if columns else '*'} FROM {table}"
            if condition:
                query += f" WHERE {condition}"
            return self.execute_query(query)
        except psycopg2.Error as e:
            logging.error(e)
            return None

    def update(self, table, set_values, condition):
        """
        Updates data in a table.

        :param table: Name of the table.
        :type table: str
        :param set_values: Dictionary of columns and values to be updated.
        :type set_values: dict
        :param condition: Condition to filter which rows will be updated.
        :type condition: str
        """
        try:
            query = f"UPDATE {table} SET {', '.join([f'{column} = %s' for column in set_values.keys()])} WHERE {condition}"
            self.execute_update(query, list(set_values.values()))
        except psycopg2.Error as e:
           logging.error(e)

    def delete(self, table, condition):
        """
        Deletes data from a table.

        :param table: Name of the table.
        :type table: str
        :param condition: Condition to filter which rows will be deleted.
        :type condition: str
        """
        try:
            query = f"DELETE FROM {table} WHERE {condition}"
            self.execute_update(query)
        except psycopg2.Error as e:
            logging.error(e)