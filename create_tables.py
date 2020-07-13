import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    drop tables

    cur
        executes commands
    conn
        connects to sql database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates tables

    cur
        executes commands
    conn
        connects to sql database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Execute table creation pipeline
    1. Grab credentals from config parser
    2. Connect to database in Redshift
    3. drop tables if they exist, create new tables

    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())) #here we can connect using the dsn perameter as one string.
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
