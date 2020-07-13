import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries



def load_staging_tables(cur, conn):
    """Load data from S3 to staging tables

       cur
           executes commands
       conn
           connects to sql database
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging tables

    cur
        executes commands
    conn
        connects to sql database
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def main():
    """
    Execute ETL pipeline
    1. Grab credentals from config parser
    2. Connect to database in Redshift
    3. Copy data from public S3 buckets into Redshift DataWarehouse
    4. Run sql qureies to incert data into facts and dimension tables.


    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())) #here we can connect using the dsn perameter as one string.
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
