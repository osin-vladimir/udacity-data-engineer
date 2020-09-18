class SqlQueries:
    songplay_table_insert = ("""insert into songplays(
                                                    start_time,
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
