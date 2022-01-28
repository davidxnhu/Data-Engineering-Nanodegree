import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events 
(
        artist        VARCHAR,
        auth          VARCHAR,
        firstName     VARCHAR,
        gender        VARCHAR,
        itemInSession INTEGER,
        lastName      VARCHAR,
        length        FLOAT, 
        level         VARCHAR,
        location      VARCHAR,
        method        VARCHAR,
        page          VARCHAR,
        registration  FLOAT, 
        sessionId     INTEGER,
        song          VARCHAR,
        status        INTEGER,
        ts            TIMESTAMP,
        userAgent     VARCHAR,
        userId        INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(
        song_id           VARCHAR PRIMARY KEY,
        artist_id         VARCHAR,
        artist_latitude   FLOAT, 
        artist_location   VARCHAR, 
        artist_longitude  FLOAT, 
        artist_name       VARCHAR,
        duration          FLOAT, 
        num_songs         INTEGER,
        title             VARCHAR,
        year              INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
    songplay_id int IDENTITY(0,1) PRIMARY KEY sortkey, 
    start_time timestamp NOT NULL REFERENCES time(start_time), 
    user_id int NOT NULL REFERENCES users(user_id), 
    level varchar, 
    song_id varchar NOT NULL REFERENCES songs(song_id), 
    artist_id varchar NOT NULL REFERENCES artists(artist_id), 
    session_id int, 
    location varchar, 
    user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
    user_id int PRIMARY KEY distkey, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(    
    song_id varchar PRIMARY KEY sortkey, 
    title varchar, 
    artist_id varchar, 
    year int, 
    duration decimal
)
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(
    artist_id varchar PRIMARY KEY distkey, 
    name varchar, 
    location varchar, 
    latitude float, 
    longitude float
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(
    start_time timestamp PRIMARY KEY sortkey distkey, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday varchar
);
""")

# Staging tables

staging_events_copy = """
COPY staging_events 
FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region 'us-west-2'
FORMAT AS json {}
timeformat 'epochmillisecs'
""".format(config.get('S3',       'LOG_DATA'),
           config.get('IAM_ROLE', 'ARN'),
           config.get('S3',       'LOG_JSONPATH')
          )


staging_songs_copy = """
COPY staging_songs 
FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region 'us-west-2'
FORMAT AS json 'auto'
""".format(config.get('S3',       'SONG_DATA'), 
           config.get('IAM_ROLE', 'ARN')
          )



# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT se.ts as start_time,
                se.userId as user_id,
                se.level as level,
                ss.song_id as song_id,
                ss.artist_id as artist_id,
                se.sessionId as session_id,
                se.location as location,
                se.userAgent as user_agent
from staging_events se JOIN staging_songs ss
ON se.artist = ss.artist_name AND se.song = ss.title
where se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userID as user_id,
                firstName as first_name,
                lastName as last_name,
                gender as gender,
                level as level
from staging_events
where userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id as song_id,
                title as title,
                artist_id as artist_id,
                year as year,
                duration as duration
from staging_songs
where song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id as artist_id,
                artist_name as name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
from staging_songs
where artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts as start_time,
                extract (hour from ts) as hour,
                extract (day from ts) as day,
                extract (week from ts) as week,
                extract (month from ts) as month,
                extract (year from ts) as year,
                extract (weekday from ts) as weekday
from staging_events
where ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,  songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

