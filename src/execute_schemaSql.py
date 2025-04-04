import mysql.connector
from mysql.connector import Error
from config.mysql_config import get_database_config
from pathlib import Path


DATABASE_NAME = "github_data"
SQL_FILE_PATH = Path("/home/nguyenphuc/Documents/DataEngineerAnhDat/DE-ETL-DataTransformation101/src/schema.sql")

def connect_to_mysql(config):
    try:
        connection =  mysql.connector.connect(**config)
        return connection
    except Error as e:
        raise Exception(f"------------can not connect to MySQL: {e}---------------") from e

def create_database(cursor, db_name):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    print(f"------------------------database '{db_name}' created-----------------------")

def execute_sql_file(cursor, file_path):
    with file_path.open('r') as file:
        sql_script = file.read()
    commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip() ]

    for cmd in commands:
        try:
            cursor.execute(cmd)
            print(f"------------------executed: {cmd.strip()[::50]}---------------")
        except Error as e:
            print(f"------------can not execute to sql cmd: {e}---------------")


def main():
    try:
        # get database config
        db_config = get_database_config()
        # remove database from config because I want to connect mysql db
        initial_config = {k : v for k,v in db_config.items() if k != "database" }

        # connect to mysql
        connection = connect_to_mysql(initial_config)
        cursor = connection.cursor()

        # create database
        create_database(cursor, DATABASE_NAME)
        connection.database = DATABASE_NAME

        #execute SQL
        execute_sql_file(cursor, SQL_FILE_PATH)
        connection.commit()
        print("---------------------- file Schema.sql executed sucessfully----------------------")
    except Exception as e:
        print(f"------------------------Error: {e}----------------------")
        if connection and connection.is_connected():
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("---------------Disconnect to mySQL--------------------")


if __name__ == "__main__":
    main()