import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function is responsable for load data into staging tables.
    This function executes the copy_table_queries in sql_queries file.

    INPUTS: 
    * cur the cursor variable
    * conn the connection with redshift database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function is responsable for inset data into dimension and fact tables.
    This function executes the insert_table_queries in sql_queries file.

    INPUTS: 
    * cur the cursor variable
    * conn the connection with redshift database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function is responsable for create the connection to redshift database
    and executes load_staging_tables and insert_tables functions.
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