# Imports
# From standard library
import configparser

# From third-party libraries
import psycopg2

# From this project
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cursor, connection):
    """Drop all tables (staging, dimensional, and fact tables)"""
    for query in drop_table_queries:
        cursor.execute(query)
        connection.commit()


def create_tables(cursor, connection):
    """Create all tables (staging, dimensional, and fact tables)"""
    for query in create_table_queries:
        cursor.execute(query)
        connection.commit()


def main():
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read("dwh.cfg")
    print(*config["REDSHIFT"].values())

    # Connect to the Redshift cluster and get cursor
    connection = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["REDSHIFT"].values()
        )
    )
    cursor = connection.cursor()

    # Drop to have a clean slate
    drop_tables(cursor, connection)

    # Create tables
    create_tables(cursor, connection)

    connection.close()


if __name__ == "__main__":
    main()
