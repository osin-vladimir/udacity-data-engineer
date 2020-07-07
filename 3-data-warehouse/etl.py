import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data into staging tables in AWS Redshift
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data points into tables in AWS Redshift
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Reads AWS config file, creates desired tables and inserts data.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} "
                            "port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print("Loading data into staging tables.")
    load_staging_tables(cur, conn)
    print("Data successfully uploaded into staging tables.")

    print("Inserting data into fact and dimensional tables.")
    insert_tables(cur, conn)
    print("Data successfully inserted to the tables.")

    conn.close()


if __name__ == "__main__":
    main()
