import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create spark session.
    Returns:
        spark (SparkSession) - spark session connected to AWS EMR cluster
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Load song data from S3 and extracts the song and artist tables
    
    Keyword arguments:
    spark -- the spark session
    input_data -- the path of input song data from S3
    output_data -- the path of output data to S3
    
    '''
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*json"
    
    # read song data file
    df = spark.read.json(song_data).drop_duplicates()

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.repartition(col("year"),col("artist_id"))
    songs_table.write.parquet(output_data + "songs_table.parquet", mode="overwrite", partitionBy=["year", "artist_id"])

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists_table.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    '''
    Load log data from S3 and extracts the user, time and songplay tables
    
    Keyword arguments:
    spark -- the spark session
    input_data -- the path of input log data from S3
    output_data -- the path of output data to S3
    
    '''
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*json"

    # read log data file
    df = spark.read.json(log_data).drop_duplicates() 
    
    # filter by actions for song plays
    df = df.filter(df.page=="NextSong")

    # extract columns for users table    
    user_table = df.select("userId", "firstName", "lastName", "gender", "level").distinct()
       
    # write users table to parquet files
    user_table.write.parquet(output_data + "user_table.parquet", mode = "overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(int(ts)/1000), T.TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts")) 
    
    # extract columns to create time table
    time_table = df.withColumn("hour",     F.hour("start_time"))\
                    .withColumn("day",     F.dayofmonth("start_time"))\
                    .withColumn("week",    F.weekofyear("start_time"))\
                    .withColumn("month",   F.month("start_time"))\
                    .withColumn("year",    F.year("start_time"))\
                    .withColumn("weekday", F.dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.repartition(col("year"),col("month"))
    time_table.write.parquet(output_data + "time_table.parquet", mode="overwrite", partitionBy = ["year", "month"])

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table.parquet")
    
    # read in artist data to use for songplays table
    artist_df = spark.read.parquet(output_data + "artists_table.parquet")

    # joined song and artist table
    df_joined_songs_artists = song_df.join(artist_df, 'artist_id').select("artist_id", "song_id","title", "artist_name")
    
    # extract columns from joined song, artist and log datasets to create songplays table 
    songplays_table = df.withColumn("songplay_id", F.monotonically_increasing_id())\
               .join(df_joined_songs_artists, df.artist == df_joined_songs_artists.artist_name)\
               .select("songplay_id", 
                      "start_time", 
                       col("userId").alias("user_id"),
                       "level", 
                       "song_id", 
                       "artist_id", 
                       col("sessionId").alias("session_id"), 
                       "location", 
                       col("userAgent").alias("user_agent"),
                       F.year("start_time").alias("year"),
                       F.month("start_time").alias("month")
                      ) 

    # write songplays table to parquet files partitioned by year and month
    songplays_table=songplays_table.repartition(col("year"),col("month"))
    songplays_table.write.parquet(output_data + "songplays_table.parquet", mode = "overwrite", partitionBy = ["year", "month"])


def main():
    """
    - Establishes spark session  
    
    - Process song data and load back into S3
    
    - Process log data and load back into S3

    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-xn/DataEngineering/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
