import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS staging_songplays"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                             artist VARCHAR,
                             auth VARCHAR,
                             firstName VARCHAR,
                             gender CHAR,
                             itemInSession INTEGER,
                             lastName VARCHAR,
                             length float,
                             level VARCHAR,
                             location VARCHAR,
                             method VARCHAR,
                             page VARCHAR,
                             registration FLOAT,
                             sessionId INTEGER,
                             song VARCHAR,
                             status INTEGER,
                             ts TIMESTAMP,
                             userAgent VARCHAR,
                             userId INTEGER
                             )
                             """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                            num_songs INTEGER,
                            artist_id VARCHAR,
                            artist_latitude FLOAT,
                            artist_longitude FLOAT,
                            artist_location VARCHAR,
                            artist_name VARCHAR,
                            song_id VARCHAR,
                            title VARCHAR, 
                            duration FLOAT,
                            year INTEGER
                              )
                             """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY, 
                    first_name VARCHAR,
                    last_name VARCHAR,
                    gender CHAR,
                    level VARCHAR
                     )
                     """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                    song_id VARCHAR PRIMARY KEY,
                    title VARCHAR,
                    artist_id VARCHAR,
                    year INTEGER,
                    duration FLOAT
                     )
                     """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                      artist_id VARCHAR PRIMARY KEY,
                      name VARCHAR,
                      location VARCHAR,
                      latitude FLOAT,
                      longitude FLOAT
                       )
                      """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                    start_time TIMESTAMP PRIMARY KEY,
                    hour INTEGER,
                    day INTEGER,
                    week INTEGER,
                    month INTEGER,
                    year INTEGER,
                    weekday VARCHAR
                    )
                    """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
                        start_time TIMESTAMP NOT NULL REFERENCES time(start_time),
                        user_id INTEGER  NOT NULL REFERENCES users(user_id),
                        level VARCHAR,
                        song_id VARCHAR NOT NULL REFERENCES songs(song_id),
                        artist_id VARCHAR NOT NULL REFERENCES artists(artist_id),
                        session_id INTEGER,
                        location VARCHAR,
                        user_agent VARCHAR
                         )
                         """)

# STAGING TABLES
EVENT_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')

staging_events_copy = ("""copy staging_events
                          from {}
                          credentials 'aws_iam_role={}'
                          region 'us-west-2'
                          json {}
                          timeformat as 'epochmillisecs'
                        """).format(EVENT_DATA, IAM_ROLE, LOG_JSONPATH)


staging_songs_copy = ("""copy staging_songs
                          from {}
                          credentials 'aws_iam_role={}'
                          region 'us-west-2'
                          json 'auto'
                        """).format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
         SELECT e.ts, e.userId, e.level, e.song, s.artist_id, e.sessionId, e.location, e.userAgent
         FROM staging_songs s
         JOIN staging_events e ON (e.song=s.title)
         WHERE e.page='NextSong'
         """)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT e.userId, e.firstName, e.lastName, e.gender, e.level
                        FROM staging_events e
                        WHERE e.userId IS NOT NULL
                     """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT s.song_id, s.title, s.artist_id, s.year, s.duration
                        FROM staging_songs s
                        WHERE s.song_id IS NOT NULL
                     """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT s.artist_id, s.artist_name, s.artist_location, s.artist_latitude, s.artist_longitude
                          FROM staging_songs s
                          WHERE artist_id IS NOT NULL
                       """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT e.ts,
                               EXTRACT(HOUR FROM e.ts),
                               EXTRACT(DAY FROM e.ts),
                               EXTRACT(WEEK FROM e.ts),
                               EXTRACT(MONTH FROM e.ts),
                               EXTRACT(YEAR FROM e.ts),
                               EXTRACT(DOW FROM e.ts)
                        FROM staging_events e
                        WHERE e.ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
