import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    - drops all tables in the database if they exist using the drop queries in sql_queries file
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    - creates the staging and dimensional tables in the database if they exist 
      using the create table queries in sql_queries file
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - reads the dwh.cfg file to get the parameters required to connect to the cluster
    - makes a connection to the cluster and database
    - passes the curser and connection to functions to drop the staging and dimensional tables
      if they exist then create the tables
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()