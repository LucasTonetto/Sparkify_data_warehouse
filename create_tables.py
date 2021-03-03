import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function is responsable for drop staging, dimension and fact tables in redshift.
    This function executes the drop_table_queries in sql_queries file.

    INPUTS: 
    * cur the cursor variable
    * conn the connection with redshift database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function is responsable for create staging, dimension and fact tables in redshift.
    This function executes the create_table_queries in sql_queries file.

    INPUTS: 
    * cur the cursor variable
    * conn the connection with redshift database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function is responsable for create the connection to redshift database
    and executes drop_tables and create_tables functions.
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