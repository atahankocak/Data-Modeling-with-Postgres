# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

temp_song_table_drop = "DROP TABLE IF EXISTS data_song;"
temp_log_table_drop = "DROP TABLE IF EXISTS data_log;"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
          songPlay_id SERIAL PRIMARY KEY NOT NULL
        , start_time BIGINT
        , user_id INT
        , level VARCHAR
        , song_id VARCHAR
        , artist_id VARCHAR
        , session_id INT
        , location VARCHAR
        , user_agent VARCHAR);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
          userId INT PRIMARY KEY NOT NULL
        , firstName VARCHAR
        , lastName VARCHAR
        , gender VARCHAR
        , level VARCHAR);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
          song_id VARCHAR PRIMARY KEY NOT NULL
        , title VARCHAR
        , artist_id VARCHAR
        , year INT
        , duration FLOAT);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
          artist_id VARCHAR PRIMARY KEY NOT NULL
        , name VARCHAR
        , location VARCHAR
        , latitude FLOAT
        , longitude FLOAT);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
          start_time BIGINT PRIMARY KEY
        , hour INT
        , day INT
        , week INT
        , month INT
        , year INT
        , weekday INT);
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (songPlay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

user_table_insert = ("""
    INSERT INTO users (userId, firstName, lastName, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT songs.song_id, songs.artist_id
    FROM songs
        JOIN artists ON artists.artist_id = songs.artist_id
    WHERE 1 = 1
        AND songs.title = %s     -- song title
        AND artists.name = %s    -- artist name
        AND songs.duration = %s; -- song duration
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = []
