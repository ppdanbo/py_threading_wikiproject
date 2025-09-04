import os
from sqlite3 import DatabaseError, connect, Connection, Cursor
import threading
import random
import time
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from dotenv import load_dotenv

"""Postgres worker and master scheduler.

This module provides a threaded master scheduler that consumes tasks from an
input queue and a simple Postgres worker that inserts scraped price data into
a Postgres database using SQLAlchemy.

Design notes
- The master scheduler is a threading.Thread subclass that reads items from
- The PostgresWorker encapsulates connection creation and the insert logic.
- The file intentionally keeps behavior minimal; callers control thread start
  (the class currently calls start() in __init__ to preserve previous behavior).
"""

class PostgresMasterSchedule(threading.Thread):
    """Threaded master schedule for managing tasks with Postgres."""

    def __init__(self, input_queue, **kwargs):
        if 'output_queues' in kwargs:
            kwargs.pop('output_queues')
        if 'output_queue' in kwargs:
            kwargs.pop('output_queue')
        super(PostgresMasterSchedule, self).__init__(**kwargs)
        self._input_queue = input_queue
        self.start()


    def run(self):
        # Implementation for managing master schedule with Postgres
        while True:
            try:
                val = self._input_queue.get() # [symbol, price, scrapped_time] , (symbol, price, scrapped_time)
            except Exception as e:
                print(f"Postgres master schedule has exception as {e}, stopping")
                break
            
            # Process the task with PostgresWorker
            symbol, price, scrapped_time = val
            if symbol == "DONE":
                print("Postgres master schedule received DONE signal, stopping")
                break
                # return
                     
            postgres_worker = PostgresWorker( symbol, price, scrapped_time)
            postgres_worker.insert_into_db()
            print(f"Postgres master schedule processed task {val}")
            # time.sleep(random.random())  # to avoid hitting Postgres too fast


class PostgresWorker:
    """Worker class for handling Postgres related tasks."""

    def __init__(self, symbol, price, scrapped_time):
        self._symbol = symbol
        self._price = price
        self._scrapped_time = scrapped_time

        load_dotenv()  # take environment variables from .env.
        self._PG_USER = os.environ.get("PG_USER", "postgres")
        self._PG__PWD = os.environ.get("PG_PWD")
        self._PG_HOST = os.environ.get("PG_HOST", "localhost")
        self._PG_PORT = os.environ.get("PG_PORT")
        self._PG_DB = os.environ.get("PG_DBNAME")
        self._connection_string = f"""postgresql+psycopg2://{self._PG_USER}:{self._PG__PWD}@{self._PG_HOST}:{self._PG_PORT}/{self._PG_DB}"""
        # print(f"PostgresWorker using connection string: {self._connection_string}")
        self._engine = create_engine(self._connection_string)
        # self._engine = create_engine(
        #     "postgresql+psycopg2://",
        #     username = self._PG_USER,
        #     password = self._PG__PWD,
        #     host = self._PG_HOST,
        #     port = self._PG_PORT,   
        #     database = self._PG_DB
        # )


    def _create_insert_query(self):
        """Return parameterized SQL insert statement for the prices table."""    
        SQL_STMT = """INSERT INTO prices (symbol, price, ingest_date) 
                        VALUES (:symbol, :price, CAST(:ingest_date AS timestamp))                    
                   """
        return SQL_STMT


    def insert_into_db(self):
        insert_query = self._create_insert_query()
        try: 
            with self._engine.connect() as connection:
                connection.execute(
                    text(insert_query), {"symbol": self._symbol, "price": self._price, "ingest_date": self._scrapped_time}
                )
                connection.commit()
            print(f"Inserted data for symbol {self._symbol} into Postgres database.")
        except Exception as e:
            print(f"Failed to insert data for {self._symbol} into Postgres: {e}")