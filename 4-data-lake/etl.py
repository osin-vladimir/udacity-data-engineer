import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process input song data and creates songs and artist dimensional tables as partitioned parquet files
    in table directrories in S3.
    """
    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data).drop_duplicates()

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(["year", "artist_id"]).parquet(f"{output_data}songs_table", mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).drop_duplicates()

    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}artists_table", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Process input log data and creates time, users and song_plays tables as partitioned parquet files
    in table directrories in S3.
    """
    # get filepath to log data file
    log_data = f"{input_data}log-data/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"]).drop_duplicates()

    # write users table to parquet files
    users_table.write.parquet(f"{output_data}users/", mode="overwrite")

    # create datetime column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))

    # extract columns to create time table
    time_table = df.withColumn("hour",    hour("start_time"))\
                   .withColumn("day",     dayofmonth("start_time"))\
                   .withColumn("week",    weekofyear("start_time"))\
                   .withColumn("month",   month("start_time"))\
                   .withColumn("year",    year("start_time"))\
                   .withColumn("weekday", dayofweek("start_time"))\
                   .select("ts", col("start_time").alias("datetime"), "hour", "day", "week", "month", "year", "weekday").drop_duplicates()

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(f"{input_data}songs_table/*/*/*")

    # read in artist data to use for songplays table
    artists_df = spark.read.parquet(f"{input_data}artists_table/*")

    # adding song information to each log
    df = df.join(songs_df, (df.song == songs_df.title))
    df.head()

    # adding artist information to each log
    df = df.join(artists_df, (df.artist == artists_df.artist_name))
    df.head()

    # adding timetable to the logs
    song_plays_table = df.join(time_table, df.start_time == time_table.datetime, 'left')
    song_plays_table.head()

    # selecting necessary elements
    song_plays_table = song_plays_table.select(monotonically_increasing_id().alias("songplay_id"),
                                               "start_time",
                                               col("userId").alias("user_id"),
                                               "level",
                                               "song_id",
                                               "artist_id",
                                               col("sessionId").alias("session_id"),
                                               "location",
                                               col("userAgent").alias("user_agent"),
                                               "year",
                                               "month").drop_duplicates()

    # write songplays table to parquet files partitioned by year and month
    song_plays_table.write.partitionBy(["year", "month"]).parquet(f"{output_data}songplays/", mode="overwrite")


def main():
    """
    Extracts data from S3 and transforms into fact and dimensional tables in parquet format
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
