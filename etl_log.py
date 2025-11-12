"""
Collect logs and insert them once at a time
"""

from enum import Enum

import psycopg2 as db
from psycopg2.extras import execute_values


class LogLevel(Enum):
    LOW = 1
    MIDDLE = 2
    HIGH = 3


class LogStatus(Enum):
    AUTO_FIXED = 1
    NEED_REVIEW = 2


class InitLogger:

    conn = None
    cursor = None
    logs = []

    def __init__(self):
        self.conn = db.connect(
            host="localhost",
            database="steam_games_trans_logs",
            user="user",
            password="12345678",
            port=5432
        )
        self.cursor = self.conn.cursor()

    def push_log(self, game_code, field_name, original_value, level: LogLevel, status: LogStatus):
        self.logs.append((game_code, field_name, original_value, level.value, status.value))

    def store_logs(self):
        sql = """
            insert into in_oltp_logs
            (game_code, field_name, original_value, level, status)
            values %s;
        """
        # the last param is tuple list ????????
        execute_values(self.cursor, sql, self.logs)
        self.logs = []
    
    def finish(self):
        self.cursor.close()
        self.conn.commit()
        self.conn.close()


class TransLogger:

    conn = None
    cursor = None
    logs = []

    def __init__(self):
        self.conn = db.connect(
            host="localhost",
            database="steam_games_trans_logs",
            user="user",
            password="12345678",
            port=5432
        )
        self.cursor = self.conn.cursor()

    def push_log(self, database_name, table_name, field_name, original_value, level, status):
        self.logs.append((database_name, table_name, field_name, original_value, level, status))

    def store_logs(self):
        sql = """
            insert into in_olap_logs
            (database_name, table_name, field_name, original_value, level, status)
            values %s;
        """
        # the last param is tuple list 
        execute_values(self.cursor, sql, self.logs)
        self.logs = []
    
    def finish(self):
        self.cursor.close()
        self.conn.commit()
        self.conn.close()

    




def main():
    pass



if __name__ == "__main__":
    main()