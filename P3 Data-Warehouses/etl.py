import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    
    Load data using the queries in `copy_table_queries` list

    Keyword arguments:
    cur -- the database cursor
    conn -- the database connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load data using the queries in `insert_table_queries` list

    Keyword arguments:
    cur -- the database cursor
    conn -- the database connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Establishes connection with the database on Amazon Redshift, and gets
    cursor to it.     
    
    - Load data from S3 to staging tables on Redshift
    
    - Load data from staging tables to analytics tables on Redshift
    
    - Finnaly close the connection

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