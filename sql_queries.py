# Import libraries
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
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar,
    auth varchar,
    firstName varchar(100),
    gender varchar(1),
    itemInSession int,
    lastName varchar(100),
    length numeric,
    level varchar(10),
    location varchar,
    method varchar(10),
    page varchar,
    registration varchar,
    sessionId int,
    song varchar,
    status int,
    ts timestamp,
    userAgent varchar,
    userId int
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int, 
    artist_id varchar, 
    artist_latitude numeric, 
    artist_longitude numeric, 
    artist_location varchar, 
    artist_name varchar, 
    song_id varchar, 
    title varchar, 
    duration numeric, 
    year int
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id bigint identity(0, 1), 
    start_time timestamp NOT NULL, 
    user_id int NOT NULL, 
    level varchar, 
    song_id varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    session_id int, 
    location varchar, 
    user_agent varchar,    
    primary key(songplay_id)
    )
    distkey(start_time)
    compound sortkey(user_id, artist_id, song_id)
    ;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY, 
    first_name varchar(100), 
    last_name varchar(100), 
    gender varchar(1), 
    level varchar(10) NOT NULL
    )
    sortkey(level)
    ;
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id varchar PRIMARY KEY, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL,
    year int, 
    duration numeric
    )
    compound sortkey(artist_id, year)
    ;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    name varchar NOT NULL, 
    location varchar, 
    latitude numeric, 
    longitude numeric
    )
    sortkey(name)
    ;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday varchar(9)
    )
    distkey(start_time)
    ;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {} 
IAM_ROLE {}
REGION 'us-west-2' 
FORMAT as JSON {}    
TIMEFORMAT as 'epochmillisecs'
;
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {} 
IAM_ROLE {}
REGION 'us-west-2' 
FORMAT as JSON 'auto ignorecase'
;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(select 
        e.ts as start_time, 
        e.userId as user_id, 
        e.level as level, 
        nvl(s.song_id, 'n/a') as song_id, 
        nvl(s.artist_id, 'n/a') as artist_id, 
        e.sessionId as session_id, 
        e.location as location, 
        e.userAgent as user_agent
from staging_events e
     left join staging_songs s
     on (e.song = s.title 
         and e.artist = s.artist_name
         and e.length = s.duration)
where e.ts is not null
and e.userId is not null
and e.page = 'NextSong'
)
""")


user_table_insert = ("""
INSERT INTO users
(select DISTINCT
        userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender as gender,
        level as level
from staging_events
where userId is not null
and level is not null
)
""")

song_table_insert = ("""
INSERT INTO songs
(select DISTINCT
        song_id as song_id,
        title as title,
        artist_id as artist_id,
        year as year,
        duration as duration
from staging_songs
where song_id is not null
and title is not null 
and artist_id is not null
)
""")

artist_table_insert = ("""
INSERT INTO artists
(select DISTINCT
        artist_id as artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
from staging_songs
where artist_id is not null
and artist_name is not null
)
""")

time_table_insert = ("""
INSERT INTO time
(select DISTINCT
        ts as start_time,
        EXTRACT(hour FROM ts) as hour,
        EXTRACT(day FROM ts) as day,
        EXTRACT(week FROM ts) as week,
        EXTRACT(month FROM ts) as month,
        EXTRACT(year FROM ts) as year,
        EXTRACT(weekday FROM ts) as weekday
from staging_events
where ts is not null)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
