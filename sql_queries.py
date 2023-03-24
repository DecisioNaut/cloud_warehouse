import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

# Staging Tables
stating_logs_table_drop = """
DROP TABLE IF EXISTS log_data CASCADE;
"""

staging_songs_table_drop = """
DROP TABLE IF EXISTS song_data CASCADE;
"""

# Dimension Tables
time_table_drop = """
DROP TABLE IF EXISTS time CASCADE;
"""

user_table_drop = """
DROP TABLE IF EXISTS users CASCADE;
"""

artist_table_drop = """
DROP TABLE IF EXISTS artists CASCADE;
"""

song_table_drop = """
DROP TABLE IF EXISTS songs CASCADE;
"""

# Fact Table
songplay_table_drop = """
DROP TABLE IF EXISTS songplays CASCADE;
"""

# CREATE TABLES

# Staging Tables
staging_logs_table_create = """
CREATE TABLE IF NOT EXISTS log_data (
    artist          VARCHAR(200)    NULL,
    auth            VARCHAR(50)     NOT NULL,
    firstName       VARCHAR(50)     NULL,
    gender          CHAR(1)         NULL,
    itemInSession   INTEGER         NOT NULL,
    lastName        VARCHAR(50)     NULL,
    length          FLOAT           NULL,
    level           CHAR(4)         NOT NULL,
    location        VARCHAR(200)    NULL,
    method          VARCHAR(10)     NOT NULL,
    page            VARCHAR(50)     NOT NULL,
    registration    FLOAT           NULL,
    sessionId       INTEGER         NOT NULL,
    song            VARCHAR(200)    NULL,
    status          INTEGER         NOT NULL,
    ts              BIGINT          NOT NULL,
    userAgent       VARCHAR(200)    NULL,
    userId          INTEGER         NULL,
    UNIQUE          (sessionId, itemInSession),
    PRIMARY KEY     (sessionId, itemInSession)
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS song_data (
    artist_id       VARCHAR(50)     NOT NULL,
    artist_latitude FLOAT           NULL,
    artist_location VARCHAR(200)    NULL,
    artist_longitude FLOAT          NULL,
    artist_name     VARCHAR(200)    NOT NULL,
    duration        FLOAT           NOT NULL,
    num_songs       INTEGER         NOT NULL,
    song_id         VARCHAR(50)     NOT NULL,
    title           VARCHAR(200)    NOT NULL,
    year            INTEGER         NOT NULL,
    UNIQUE          (artist_id, song_id),
    PRIMARY KEY     (artist_id, song_id)
);
"""

# Dimension Tables
time_table_create = """
CREATE TABLE IF NOT EXISTS time (
    start_time      TIMESTAMP       NOT NULL,
    year            INTEGER         NOT NULL,
    month           INTEGER         NOT NULL,
    day             INTEGER         NOT NULL,
    hour            INTEGER         NOT NULL,
    week            INTEGER         NOT NULL,
    weekday         INTEGER         NOT NULL,
    UNIQUE          (start_time),
    PRIMARY KEY     (start_time)
)
DISTSTYLE EVEN
SORTKEY (start_time);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
    user_id         INTEGER         NOT NULL,
    first_name      VARCHAR(50)     NOT NULL,
    last_name       VARCHAR(50)     NOT NULL,
    gender          CHAR(1)         NOT NULL,
    level           CHAR(4)         NOT NULL,
    UNIQUE          (user_id),
    PRIMARY KEY     (user_id)
)
DISTSTYLE ALL
SORTKEY (last_name, first_name, gender, level);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id       INTEGER         NOT NULL,
    name            VARCHAR(200)    NOT NULL,
    location        VARCHAR(200)    NULL,
    latitude        FLOAT           NULL,
    longitude       FLOAT           NULL,
    UNIQUE          (artist_id),
    PRIMARY KEY     (artist_id)
)
DISTSTYLE ALL
SORTKEY (name);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS songs (
    song_id         INTEGER         NOT NULL,
    title           VARCHAR(200)    NOT NULL,
    artist_id       INTEGER         NOT NULL,
    year            INTEGER         NULL,
    duration        FLOAT           NULL,
    PRIMARY KEY     (song_id),
    UNIQUE          (song_id),
    FOREIGN KEY     (artist_id)     REFERENCES  artists (artist_id)
)
DISTSTYLE ALL
SORTKEY (title, year);
"""

# Fact Table
songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
    session_id      INTEGER         NOT NULL,
    songplay_id     INTEGER         NOT NULL,
    start_time      TIMESTAMP       NOT NULL,
    artist_id       INTEGER         NOT NULL,
    song_id         INTEGER         NOT NULL,
    user_id         INTEGER         NOT NULL,
    level           CHAR(4)         NOT NULL,
    location        VARCHAR(200)    NOT NULL,
    user_agent      VARCHAR(200)    NOT NULL,
    PRIMARY KEY (session_id, songplay_id),
    UNIQUE (session_id, songplay_id),
    FOREIGN KEY (start_time)        REFERENCES  time (start_time),
    FOREIGN KEY (user_id)           REFERENCES  users (user_id),
    FOREIGN KEY (artist_id)         REFERENCES  artists (artist_id),
    FOREIGN KEY (song_id)           REFERENCES  songs (song_id)
)
DISTSTYLE EVEN
SORTKEY (start_time);
"""


# COPY DATA INTO STAGING TABLES

stating_logs_copy = f"""
COPY log_data
FROM {config["S3"]["log_data"]}
IAM_ROLE {config["IAM_ROLE"]["ARN"]}
JSON {config["S3"]["log_jsonpath"]}
"""

staging_songs_copy = f"""
COPY song_data
FROM {config["S3"]["song_data"]}
IAM_ROLE {config["IAM_ROLE"]["ARN"]}
FORMAT AS JSON 'auto'
"""

# INSERT INTO FINAL TABLES

# Dimension Tables

time_table_insert = """
INSERT INTO time
SELECT
    TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS year,
    EXTRACT(month FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS month,
    EXTRACT(day FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS day,
    EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS hour,
    EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS week,
    EXTRACT(dow FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS weekday
FROM
    log_data
WHERE
    auth = 'Logged In' AND
    length > 0
GROUP BY
    start_time
ORDER BY
    start_time
;
"""

user_table_insert = """
WITH
    user_log_data AS (
        SELECT
            userId AS user_id,
            firstName AS first_name,
            lastName AS last_name,
            gender,
            level,
            TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS time
        FROM
            log_data
        WHERE
            auth = 'Logged In' AND
            length > 0
    ),
    max_user_times AS (
        SELECT
            user_id,
            MAX(time) AS max_time
        FROM
            user_log_data
        GROUP BY
            user_id
    )
--- 
INSERT INTO users
SELECT
    user_log_data.user_id,
    user_log_data.first_name,
    user_log_data.last_name,
    user_log_data.gender,
    user_log_data.level
FROM
    user_log_data
JOIN
    max_user_times
ON
    user_log_data.user_id = max_user_times.user_id AND
    user_log_data.time = max_user_times.max_time;
"""

artist_table_insert = """
WITH
    log_data_artists AS (
        SELECT DISTINCT
            artist AS name
        FROM
            log_data
        WHERE
            auth = 'Logged In' AND
            length > 0
    ),
    song_data_artists AS (
        SELECT DISTINCT
            artist_name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
        FROM
            song_data
        ORDER BY
            location DESC,
            latitude DESC,
            longitude DESC
    )
---
INSERT INTO artists
SELECT
    ROW_NUMBER() OVER () AS artist_id,
    log_data_artists.name,
    song_data_artists.location,
    song_data_artists.latitude,
    song_data_artists.longitude
FROM
    log_data_artists
LEFT JOIN
    song_data_artists
ON
    log_data_artists.name = song_data_artists.artist_name;

"""


song_table_insert = """
WITH log_data_songs AS (
    SELECT
        song AS title,
        artist
    FROM
        log_data
    WHERE
        auth = 'Logged In' AND
        length > 0
    GROUP BY
        title,
        artist
),
artist_ids AS (
    SELECT
        name,
        artist_id
    FROM
        artists
),
log_data_songs_with_artist_id AS (
    SELECT
        log_data_songs.title,
        log_data_songs.artist,
        artist_ids.artist_id
    FROM
        log_data_songs
    LEFT JOIN
        artist_ids
    ON log_data_songs.artist = artist_ids.name
),
song_data_ranked AS (
    SELECT
        title,
        artist_name,
        CASE
            WHEN year = 0 THEN NULL
            ELSE year
        END AS year,
        duration,
        DENSE_RANK() OVER (PARTITION BY title, artist_name ORDER BY duration DESC) AS rank
    FROM
        song_data
)
---
INSERT INTO songs
SELECT
    ROW_NUMBER() OVER () AS song_id,
    log_data_songs_with_artist_id.title,
    log_data_songs_with_artist_id.artist_id,
    song_data_ranked.year,
    song_data_ranked.duration
FROM
    log_data_songs_with_artist_id
LEFT JOIN
    song_data_ranked
ON
    log_data_songs_with_artist_id.title = song_data_ranked.title AND
    log_data_songs_with_artist_id.artist = song_data_ranked.artist_name
WHERE
    song_data_ranked.rank = 1;
"""

# Fact Table
songplay_table_insert = """
WITH
    raw_log_data AS (
        SELECT
            sessionId AS session_id,
            itemInSession AS item_in_session,
            TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
            artist,
            song,
            userId AS user_id,
            level,
            location,
            userAgent AS user_agent
        FROM
            log_data
        WHERE
            auth = 'Logged In' AND
            length > 0
    ),
    raw_artist_data AS (
        SELECT
            artist_id,
            name
        FROM
            artists
    ),
    raw_song_data AS (
        SELECT
            song_id,
            title,
            artist_id
        FROM
            songs
    )

INSERT INTO songplays (
    session_id,
    songplay_id,
    start_time,
    artist_id,
    song_id,
    user_id,
    level,
    location,
    user_agent
)
SELECT
    raw_log_data.session_id,
    raw_log_data.item_in_session,
    raw_log_data.start_time,
    raw_artist_data.artist_id,
    raw_song_data.song_id,
    raw_log_data.user_id,
    raw_log_data.level,
    raw_log_data.location,
    raw_log_data.user_agent
FROM
    raw_log_data
LEFT JOIN
    raw_artist_data
ON
    raw_log_data.artist = raw_artist_data.name
LEFT JOIN
    raw_song_data
ON
    raw_log_data.song = raw_song_data.title AND
    raw_artist_data.artist_id = raw_song_data.artist_id;
"""

# QUERY LISTS for import from etl.py

# Drop Tables
drop_table_queries = [
    # Staging Tables
    stating_logs_table_drop,
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
    staging_logs_table_create,
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
copy_table_queries = [stating_logs_copy, staging_songs_copy]

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
