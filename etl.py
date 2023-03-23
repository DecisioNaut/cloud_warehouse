# Imports
# From standard library
import configparser

# From third-party libraries
import psycopg2

# From this project
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cursor, connnection):
    """Load staging tables from S3 using the copy tables in sql_queries.py"""
    for query in copy_table_queries:
        cursor.execute(query)
        connnection.commit()


def insert_tables(cursor, connection):
    """Insert data into the dimensional and fact tables using the insert queries in sql_queries.py"""
    for query in insert_table_queries:
        cursor.execute(query)
        connection.commit()


def main():
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    # Connect to the Redshift cluster and get cursor
    connection = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cursor = connection.cursor()

    # Load staging tables
    load_staging_tables(cursor, connection)

    # Insert data into dimensional and fact tables
    insert_tables(cursor, connection)

    # Close the connection
    connection.close()


if __name__ == "__main__":
    main()
