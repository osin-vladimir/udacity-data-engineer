import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh_local.cfg')
DWH_ROLE_ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")

# DROP TABLES
staging_events_table_drop = 'drop table if exists staging_events'
staging_songs_table_drop = 'drop table if exists staging_songs'
songplay_table_drop = 'drop table if exists songplays'
user_table_drop = 'drop table if exists users'
song_table_drop = 'drop table if exists songs'
artist_table_drop = 'drop table if exists artists'
time_table_drop = 'drop table if exists time'

# CREATE TABLES
staging_events_table_create = ("""create table if not exists staging_events
                                (artist          varchar,
                                 auth            varchar,
                                 firstName       varchar,
                                 gender          varchar,
                                 itemInSession   integer,
                                 lastName        varchar,
                                 length          float,
                                 level           varchar,
                                 location        varchar,
                                 method          varchar,
                                 page            varchar  ,
                                 registration    varchar,
                                 sessionId       integer,
                                 song            varchar,
                                 status          integer,
                                 ts              timestamp,
                                 userAgent       varchar,
                                 userId          integer); """)

staging_songs_table_create = ("""create table if not exists staging_songs
                                (num_song          integer,
                                 artist_id         varchar,
                                 artist_latitude   float,
                                 artist_longitude  float,
                                 artist_location   varchar,
                                 artist_name       varchar,
                                 song_id           varchar,
                                 title             varchar,
                                 duration          float,
                                 year              integer); """)

songplay_table_create = ("""create table if not exists songplays
                           (songplay_id  integer identity(0,1) primary key,
                            start_time   timestamp not null,
                            user_id      integer   not null,
                            level        varchar,
                            song_id      varchar   not null,
                            artist_id    varchar   not null,
                            session_id   integer,
                            location     varchar,
                            user_agent   varchar); """)

user_table_create = ("""create table if not exists users
                        (user_id    integer primary key,
                         last_name  varchar,
                         first_name varchar,
                         gender     varchar,
                         level      varchar );""")

song_table_create = ("""create table if not exists songs
                        (song_id   varchar  primary key,
                         title     varchar,
                         artist_id varchar,
                         year      integer,
                         duration  float
                        ); """)

artist_table_create = ("""create table if not exists artists
                          (artist_id varchar  primary key,
                           name      varchar,
                           location  varchar,
                           latitude float,
                           longitude float
                          ); """)

time_table_create = ("""create table if not exists time
                        (
                          start_time   timestamp  primary key,
                          hour         integer,
                          day          integer,
                          week         integer,
                          month        integer,
                          year         integer,
                          weekday      integer
                          );""")

# STAGING TABLES

staging_events_copy = (f"""copy staging_events from {LOG_DATA}
                           credentials 'aws_iam_role={DWH_ROLE_ARN}'
                           compupdate off region 'us-west-2'
                           timeformat as 'epochmillisecs'
                           truncatecolumns blanksasnull emptyasnull
                           format as json {LOG_JSONPATH};""")


staging_songs_copy = (f"""copy staging_songs FROM {SONG_DATA}
                          credentials 'aws_iam_role={DWH_ROLE_ARN}'
                          compupdate off region 'us-west-2'
                          truncatecolumns blanksasnull emptyasnull
                          format as json 'auto';""")

# FINAL TABLESx
songplay_table_insert = ("""insert into songplays(start_time,
                                                  user_id,
                                                  level,
                                                  song_id,
                                                  artist_id,
                                                  session_id,
                                                  location,
                                                  user_agent)
                            select distinct
                              events.ts,
                              events.userId,
                              events.level,
                              songs.song_id,
                              songs.artist_id,
                              events.sessionId,
                              songs.artist_location,
                              events.useragent
                            from staging_events events
                            left join staging_songs songs on
                              (events.artist = songs.artist_name
                              and events.song = songs.title)
                            where songs.song_id is not null and
                                  events.userId is not null and
                                  songs.artist_id is not null and
                                  events.ts is not null and events.page = 'NextSong' ;
                        """)

user_table_insert = ("""insert into users(user_id,
                                         first_name,
                                         last_name,
                                         gender,
                                         level)
                        select distinct
                          userId,
                          firstName,
                          lastName,
                          gender,
                          level
                        from staging_events
                        where userId is not null
                        and page = 'NextSong' ; """)

song_table_insert = ("""insert into songs(song_id,
                                          title,
                                          artist_id,
                                          year,
                                          duration)
                        select distinct
                          song_id,
                          title,
                          artist_id,
                          year,
                          duration
                        from staging_songs
                        where song_id is not null; """)

artist_table_insert = ("""insert into artists(artist_id,
                                              name,
                                              location,
                                              latitude,
                                              longitude)
                          select distinct
                            artist_id,
                            artist_name,
                            artist_location,
                            artist_latitude,
                            artist_longitude
                          from staging_songs
                          where artist_id is not null; """)

time_table_insert = ("""insert into time(start_time,
                                         hour,
                                         day,
                                         week,
                                         month,
                                         year,
                                         weekday)
                        select distinct
                          ts,
                          extract(hour from ts),
                          extract(day from ts),
                          extract(week from ts),
                          extract(month from ts),
                          extract(year from ts),
                          extract(weekday from ts)
                        from staging_events
                        where ts is not null; """)

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create,
                        artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert,
                        time_table_insert]
