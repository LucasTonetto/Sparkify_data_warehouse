import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("CREATE TABLE IF NOT EXISTS staging_events ( \
                                event_id INT IDENTITY(0,1)  NOT NULL, \
                                artist VARCHAR, \
                                auth VARCHAR, \
                                firstName VARCHAR, \
                                gender VARCHAR, \
                                itemInSession VARCHAR, \
                                lastName VARCHAR, \
                                length VARCHAR, \
                                level VARCHAR, \
                                location VARCHAR, \
                                method VARCHAR, \
                                page VARCHAR, \
                                registration BIGINT, \
                                sessionId INTEGER NOT NULL, \
                                song VARCHAR, \
                                status INTEGER, \
                                ts BIGINT NOT NULL, \
                                userAgent VARCHAR, \
                                userId INTEGER \
                            )")

staging_songs_table_create = ("CREATE TABLE IF NOT EXISTS staging_songs ( \
                                 artist_id VARCHAR, \
                                 artist_latitude FLOAT, \
                                 artist_location VARCHAR, \
                                 artist_longitude FLOAT, \
                                 artist_name VARCHAR, \
                                 duration FLOAT, \
                                 num_songs INT, \
                                 song_id VARCHAR, \
                                 title VARCHAR, \
                                 year INT \
                            )")

songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays ( \
                            songplay_id INT IDENTITY(0,1) PRIMARY KEY, \
                            start_time TIMESTAMP, \
                            user_id INT NOT NULL, \
                            level VARCHAR NOT NULL, \
                            song_id VARCHAR NOT NULL, \
                            artist_id VARCHAR NOT NULL, \
                            session_id INT NOT NULL, \
                            location VARCHAR, \
                            user_agent VARCHAR \
                        );")

user_table_create = ("CREATE TABLE IF NOT EXISTS users ( \
                        user_id INT PRIMARY KEY, \
                        first_name VARCHAR NOT NULL, \
                        last_name VARCHAR NOT NULL, \
                        gender VARCHAR NOT NULL, \
                        level VARCHAR NOT NULL \
                    );")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs ( \
                        song_id VARCHAR PRIMARY KEY, \
                        title VARCHAR NOT NULL, \
                        artist_id VARCHAR NOT NULL, \
                        year INT, \
                        duration FLOAT NOT NULL \
                    );")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists ( \
                            artist_id VARCHAR PRIMARY KEY, \
                            name VARCHAR NOT NULL, \
                            location VARCHAR, \
                            latitude FLOAT, \
                            longitude FLOAT \
                        );")

time_table_create = ("CREATE TABLE IF NOT EXISTS time ( \
                        start_time TIMESTAMP PRIMARY KEY, \
                        hour INT NOT NULL, \
                        day INT NOT NULL, \
                        week INT NOT NULL, \
                        month INT NOT NULL, \
                        year INT NOT NULL, \
                        weekday INT NOT NULL \
                    );")

# STAGING TABLES

staging_events_copy = ("COPY staging_events \
                        from {} \
                        credentials 'aws_iam_role={}' \
                        format as json {} \
                        STATUPDATE ON \
                        region 'us-west-2';").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("COPY staging_songs \
                        from {} \
                        credentials 'aws_iam_role={}' \
                        json 'auto' \
                        region 'us-west-2';").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("INSERT INTO songplays ( \
                            start_time, \
                            user_id, \
                            level, \
                            song_id, \
                            artist_id, \
                            session_id, \
                            location, \
                            user_agent \
                        ) \
                        SELECT \
                            TIMESTAMP 'epoch' + ts/1000 * interval '1 second', \
                            e.userid, \
                            e.level, \
                            s.song_id, \
                            s.artist_id, \
                            e.sessionid, \
                            e.location, \
                            e.userAgent \
                        FROM staging_events AS e \
                        JOIN staging_songs AS s ON e.artist = s.artist_name AND e.song = s.title \
                        WHERE e.page='NextSong'")

user_table_insert = ("INSERT INTO users ( \
                        user_id, \
                        first_name, \
                        last_name, \
                        gender, \
                        level \
                    ) \
                    SELECT \
                        DISTINCT(CAST(e.userid AS INT)), \
                        e.firstname, \
                        e.lastname, \
                        e.gender, \
                        e.level \
                    FROM staging_events AS e \
                    WHERE e.userid IS NOT NULL")

song_table_insert = ("INSERT INTO songs (\
                        song_id, \
                        title, \
                        artist_id, \
                        year, \
                        duration \
                    ) \
                    SELECT \
                        DISTINCT(s.song_id), \
                        e.song, \
                        s.artist_id, \
                        s.year, \
                        CAST(e.length AS FLOAT) \
                    FROM staging_events AS e \
                    JOIN staging_songs AS s ON e.artist = s.artist_name AND e.song = s.title")

artist_table_insert = ("INSERT INTO artists (\
                            artist_id, \
                            name, \
                            location, \
                            latitude, \
                            longitude \
                        ) \
                        SELECT \
                            DISTINCT(s.artist_id), \
                            s.artist_name, \
                            s.artist_location, \
                            CAST(s.artist_latitude AS FLOAT), \
                            CAST(s.artist_longitude AS FLOAT) \
                        FROM staging_songs AS s")

time_table_insert = ("INSERT INTO time (\
                        start_time, \
                        hour, \
                        day, \
                        week, \
                        month, \
                        year, \
                        weekday \
                    ) \
                    SELECT \
                        DISTINCT(s.start_time), \
                        EXTRACT (HOUR FROM s.start_time), \
                        EXTRACT (DAY FROM s.start_time), \
                        EXTRACT (WEEK FROM s.start_time), \
                        EXTRACT (MONTH FROM s.start_time), \
                        EXTRACT (YEAR FROM s.start_time), \
                        EXTRACT (WEEKDAY FROM s.start_time) \
                    FROM songplays AS s;")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
