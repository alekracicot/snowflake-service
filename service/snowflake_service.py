import pandas as pd
import snowflake.connector

from config import settings
from snowflake.connector.pandas_tools import write_pandas, pd_writer
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

class SnowflakeService:
    def __init__(self, settings=settings) -> None:
        self.user: str = settings.user
        self.account: str = settings.account
        self.authenticator: str = settings.authenticator
        self.warehouse: str = settings.warehouse
        self.database: str = settings.database
        self.schema: str = settings.schema
        self.role:str = settings.role
        self.connection = None
        self.cursor = None

    def __str__(self) -> str:
        params = vars(self)
        return "Snowflake Service \n params: {}".format(params)

    def connect(self) -> None:
        ctx = snowflake.connector.connect(
              user=self.user,
              account=self.account,
              authenticator=self.authenticator,
              warehouse=self.warehouse
              )
        
        self.connection = ctx

    def query(self, query: str, 
              auto_connect: bool=True) -> pd.DataFrame:
        if self.connection:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query)
            df = self.cursor.fetch_pandas_all()

        elif auto_connect:
            self.connect()
            self.cursor = self.connection.cursor()
            self.cursor.execute(query)
            df = self.cursor.fetch_pandas_all()

        else:
            Exception("Snowflake Service is not connected")

        self.cursor.close()
        return df

    def query_batches(self, query: str, chunk_size: int =10_000,
                      auto_connect: bool=True) -> pd.DataFrame:
        if self.connection:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query)
            df = self.cursor.fetch_pandas_all()

        elif auto_connect:
            self.connect()
            self.cursor = self.connection.cursor()
            self.cursor.execute(query)
            df = self.cursor.fetch_pandas_batches(chunk_size=chunk_size)

        else:
            Exception("Snowflake Service is not connected")

        self.cursor.close()
        return df

    def create_table(self, df: pd.DataFrame, db: str,
                     schema: str, table: str) -> None:
        
        engine = create_engine(URL(
            account = self.account,
            user = self.user,
            authenticator=self.authenticator,
            database = db,
            schema = schema,
            warehouse = self.warehouse,
            role=self.role))
        
        try:
            df.to_sql(table, con=engine, index=False, method=pd_writer)

        except ValueError:
            print('Verify if table exists')

        engine.dispose()

    def push_to_snowflake(self, df: pd.DataFrame, db: str,
                          schema: str, table: str) -> None:
        """Push a pandas DataFrame based on dtypes

        Args:
            df (pd.DataFrame): DataFrame to push
            table (str): Table Name including DB and Schema
        """

        cnx = self.connection
        cnx.cursor().execute(f"USE DATABASE {db}")
        cnx.cursor().execute(f"USE SCHEMA {schema}")
        success, nchunks, nrows, _ = write_pandas(cnx, df, table)
        print(success, nchunks, nrows, _)
