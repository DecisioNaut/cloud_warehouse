import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

# Staging Tables
staging_events_table_drop = """
"""

staging_songs_table_drop = """
"""

# Dimension Tables
time_table_drop = """
"""

user_table_drop = """
"""

artist_table_drop = """
"""

song_table_drop = """
"""

# Fact Table
songplay_table_drop = """
"""

# CREATE TABLES

# Staging Tables
staging_events_table_create = """
"""

staging_songs_table_create = """
"""

# Dimension Tables
time_table_create = """
"""

user_table_create = """
"""

artist_table_create = """
"""

song_table_create = """
"""

# Fact Table
songplay_table_create = """
"""


# COPY DATA INTO STAGING TABLES

staging_events_copy = (
    """
"""
).format()

staging_songs_copy = (
    """
"""
).format()

# INSERT INTO FINAL TABLES

# Dimension Tables

time_table_insert = """
"""

artist_table_insert = """
"""

user_table_insert = """
"""

song_table_insert = """
"""

# Fact Table
songplay_table_insert = """
"""


# QUERY LISTS for import from etl.py

# Drop Tables
drop_table_queries = [
    # Staging Tables
    staging_events_table_drop,
    staging_songs_table_drop,
    # Dimension Tables
    time_table_drop,
    user_table_drop,
    artist_table_drop,
    song_table_drop,
    # Fact Table
    songplay_table_drop,
]

# Create Tables
create_table_queries = [
    # Staging Tables
    staging_events_table_create,
    staging_songs_table_create,
    # Dimension Tables
    time_table_create,
    user_table_create,
    artist_table_create,
    song_table_create,
    # Fact Table
    songplay_table_create,
]

# Copy Data into Staging Tables
copy_table_queries = [staging_events_copy, staging_songs_copy]

# Insert Data into Final Tables
# Please note that the order of the insert queries is important
# as the song table refernces the artist table and
# the songplay table references all dimension tables
insert_table_queries = [
    # Dimension Tables
    time_table_insert,
    user_table_insert,
    artist_table_insert,
    song_table_insert,
    # Fact Table
    songplay_table_insert,
]
