class SqlQueries:
    events_table_create = ("""
    DROP TABLE IF EXISTS staging_events;
    CREATE TABLE staging_events (
        artist          TEXT,
        auth            TEXT,
        firstName       TEXT,
        gender          CHAR,
        itemInSession   INTEGER,
        lastName        TEXT,
        length          FLOAT,
        level           TEXT,
        location        TEXT,
        method          TEXT,
        page            TEXT,
        registration    BIGINT,
        sessionId       INTEGER,
        song            TEXT,
        status          INTEGER,
        ts              BIGINT,
        userAgent       TEXT,
        userId          INTEGER
    );    
    """)

    songs_table_create = ("""
    DROP TABLE IF EXISTS staging_songs;
    CREATE TABLE staging_songs (
        artist_id           TEXT,
        artist_latitude     FLOAT,
        artist_location     TEXT,
        artist_longitude    FLOAT,
        artist_name         TEXT,
        duration            FLOAT,
        num_songs           INTEGER,
        song_id             TEXT,
        title               TEXT,
        year                INTEGER
    );
    """)

    songplay_table_create = ("""
    DROP TABLE IF EXISTS songplays;
    CREATE TABLE songplays (
        songplay_id         VARCHAR         NOT NULL,
        start_time          TIMESTAMP       NOT NULL,
        user_id             INTEGER         NOT NULL,
        level               VARCHAR         NOT NULL,
        song_id             INTEGER         NOT NULL,
        artist_id           INTEGER         NOT NULL,
        session_id          INTEGER         NOT NULL,
        location            VARCHAR         NOT NULL,
        user_agent          VARCHAR         NOT NULL,
        PRIMARY KEY (songplay_id)
    );
    """)

    user_dimension_table_create = ("""
        DROP TABLE IF EXISTS dim_user;
        CREATE TABLE dim_user (
            user_id         INTEGER         NOT NULL,
            first_name      VARCHAR         NOT NULL,
            last_name       VARCHAR         NOT NULL,
            gender          CHAR(1)         NOT NULL,
            level           VARCHAR         NOT NULL,
            PRIMARY KEY (user_id)
        );
    """)

    song_dimension_table_create = ("""
        DROP TABLE IF EXISTS dim_song;
        CREATE TABLE dim_song (
            song_id         VARCHAR        NOT NULL,
            title           VARCHAR        NOT NULL,
            artist_id       VARCHAR        NOT NULL,
            year            INTEGER        NOT NULL,
            duration        FLOAT          NOT NULL,
            PRIMARY KEY (song_id)
        );
    """)

    artist_dimension_table_create = ("""
        DROP TABLE IF EXISTS dim_artist;
        CREATE TABLE dim_artist (
            artist_id           VARCHAR        NOT NULL,
            artist_name         VARCHAR        NOT NULL,
            artist_location     VARCHAR        NOT NULL,
            artist_latitude     FLOAT          NOT NULL,
            artist_longitude    FLOAT          NOT NULL,
            PRIMARY KEY (artist_id)

        );
    """)

    time_dimension_table_create = ("""
        DROP TABLE IF EXISTS dim_time;
        CREATE TABLE dim_time (
            start_time      TIMESTAMP       NOT NULL,
            hour            INTEGER         NOT NULL,
            day             INTEGER         NOT NULL,
            week            INTEGER         NOT NULL, 
            month           INTEGER         NOT NULL,
            year            INTEGER         NOT NULL,
            dayofweek       VARCHAR         NOT NULL,
            PRIMARY KEY (start_time)
        );

    """)

    songplay_table_insert = ("""
        INSERT INTO songplays (
            SELECT
                    md5(events.sessionid || events.start_time) songplay_id,
                    events.start_time, 
                    events.userid, 
                    events.level, 
                    songs.song_id, 
                    songs.artist_id, 
                    events.sessionid, 
                    events.location, 
                    events.useragent
                    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
                FROM staging_events
                WHERE page='NextSong') events
                LEFT JOIN staging_songs songs
                ON events.song = songs.title
                    AND events.artist = songs.artist_name
                    AND events.length = songs.duration
        );
    """)

    user_table_insert = ("""
        INSERT INTO dim_user (
            SELECT distinct userid, firstname, lastname, gender, level
            FROM staging_events
            WHERE page='NextSong'
        );
    """)

    song_table_insert = ("""
        INSERT INTO dim_song (
            SELECT distinct song_id, title, artist_id, year, duration
            FROM staging_songs
            WHERE song_id IS NOT NULL
        );
    """)

    artist_table_insert = ("""
        INSERT INTO dim_artist (
            SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
            FROM staging_songs
            WHERE artist_id IS NOT NULL
        );
    """)

    time_table_insert = ("""
        INSERT INTO dim_time (
            SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
                extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
            FROM songplays
        );
    """)