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
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
        artist         varchar,
        auth           varchar ,
        firstName      varchar,
        gender         char (1),
        itemInSession  int ,
        lastName       varchar,
        length         numeric,
        level          varchar ,
        location       varchar,
        method         varchar ,
        page           varchar ,
        registration   numeric,
        sessionId      int ,
        song           varchar,
        status         int ,
        ts             numeric ,
        userAgent      varchar,
        userId         int
    )
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
        num_songs          int ,
        artist_id          char (18) ,
        artist_latitude    varchar,
        artist_longitude   varchar,
        artist_location    varchar,
        artist_name        varchar,
        song_id            char (18) ,
        title              varchar ,
        duration           numeric ,
        year               int 
    )
""")

songplay_table_create = ("""
CREATE TABLE songplays (
        songplay_id  int identity(0, 1) primary key,
        start_time   timestamp ,
        user_id      int ,
        level        varchar ,
        song_id      char (18),
        artist_id    char (18),
        session_id   int ,
        location     varchar,
        user_agent   varchar 
    )
""")

user_table_create = ("""
CREATE TABLE users (
        user_id    int primary key,
        first_name varchar ,
        last_name  varchar ,
        gender     char (1) ,
        level      varchar 
    )
""")

song_table_create = ("""
CREATE TABLE songs (
        song_id    char (18) primary key,
        title      varchar ,
        artist_id  char (18) ,
        year       int ,
        duration   numeric 
    )
""")

artist_table_create = ("""
CREATE TABLE artists (
        artist_id  char (18) primary key,
        name varchar ,
        location varchar,
        latitude numeric,
        longitude numeric
    )
""")

time_table_create = ("""
CREATE TABLE times (
        start_time  timestamp not null primary key,
        hour        int       not null,
        day         int       not null,
        week        int       not null,
        month       int       not null,
        year        int       not null,
        weekday     int       not null
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    iam_role '{}'
    format as json {}
""").format(
    config.get("S3", "LOG_DATA"),
    config.get('IAM_ROLE', 'ARN'),
    config.get("S3", "LOG_JSONPATH")
)

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role '{}'
    json 'auto'
""").format(config.get("S3", "SONG_DATA"), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
        start_time, user_id, level, song_id, artist_id,
        session_id, location, user_agent
    )
SELECT
        timestamp 'epoch' + e.ts / 1000 * interval '1 second' as start_time,
        e.userId as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId as session_id,
        e.location,
        e.userAgent as user_agent
FROM   staging_events e
LEFT JOIN staging_songs s on e.song = s.title and e.artist = s.artist_name
WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users
SELECT distinct eo.userId, eo.firstName, eo.lastName, eo.gender, eo.level
FROM   staging_events eo
WHERE  eo.userId IS NOT NULL
AND    eo.page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs
SELECT distinct song_id,
        title,
        artist_id,
        year,
        duration
FROM   staging_songs
WHERE  song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT distinct artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
FROM   staging_songs
WHERE  artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO times
SELECT
        ti.start_time,
        extract(hour from ti.start_time) as hour,
        extract(day from ti.start_time) as day,
        extract(week from ti.start_time) as week,
        extract(month from ti.start_time) as month,
        extract(year from ti.start_time) as year,
        extract(weekday from ti.start_time) as weekday
FROM (
        SELECT distinct
            timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time
        FROM staging_events
        WHERE page = 'NextSong'
    ) ti
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
