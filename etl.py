import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copy data to the staging tables from JSON files

    Parameters
    ----------
    cur : psycopg2 connection cursor
        A cursor object to execute database operations
    conn : psycopg2 connection
        A DB connection

    Returns
    -------
    None
       Nothing
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Copy data from the staging tables to the fact and dimension tables

    Parameters
    ----------
    cur : psycopg2 connection cursor
        A cursor object to execute database operations
    conn : psycopg2 connection
        A DB connection

    Returns
    -------
    None
       Nothing
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    1. Connect to the database
    2. Copy data to the staging tables
    3. Copy data from the staging tables to the fact and dimension tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()