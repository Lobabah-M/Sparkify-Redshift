import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - loads the staging tables using the copy commands in sql_queries
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    - loads the dimensional tables using the insert statements in sql_queries
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - reads the dwh.cfg file to get the parameters required to connect to the cluster
    - makes a connection to the cluster and database
    - passes the curser and connection to functions to load the staging and dimensional tables
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