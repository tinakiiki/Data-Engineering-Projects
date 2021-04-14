import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col,to_timestamp 
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''get or create a spark session with the appropriate configuration'''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''read song data from a folder in S3, extract the fields for songs and artists tables then save the tables as parquet \          files'''
    
    # get filepath to song data file
    # use sub folder with one song for local testing
    song_data = input_data + "song_data/A/A/A/*.json" 
    #use whole song data folder for full run
    #song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    song_df = spark.read.json(song_data)
    
    # create temp view from song dataframe for sql querying
    song_df.createOrReplaceTempView("songs_table")
    
    # extract columns to create songs table using sql
    songs_table = spark.sql(""" SELECT  song_id
                                       , title
                                       , artist_id
                                       , year
                                       , duration
                                FROM songs_table
                                WHERE song_id IS NOT NULL
                             """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode('overwrite').parquet(output_data+"songs.parquet")

    # extract columns to create artists table
    artists_table = spark.sql(""" SELECT artist_id
                                       , artist_name
                                       , artist_location
                                       , artist_latitude
                                       , artist_longitude 
                                   FROM songs_table 
                                   WHERE artist_id IS NOT NULL
                               """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+"artists.parquet")


def process_log_data(spark, input_data, output_data):
    '''read log data from a folder in S3, extract the fields for users,songplays and time tables then save the tables as \
    parquet files'''
    
    # get filepath to log data file
    log_data = input_data + 'log-data/*/*/*.json'

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    song_plays = log_df.filter(log_df.page == "NextSong")
    
    #create temporary view for querying in sql
    song_plays.createOrReplaceTempView("song_plays")
    
    # extract columns for users table    
    users_table = spark.sql(""" SELECT userId  
                                      , firstName
                                      , lastName
                                      , gender 
                                      , level 
                                FROM song_plays
                                WHERE userId IS NOT NULL
                             """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+"users.parquet")

    # create timestamp column from original timestamp column
    log_df = log_df.withColumn("timestamp", col("ts").cast(IntegerType()))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000))
    log_df = log_df.withColumn('dateTime',get_datetime(log_df.timestamp))
    
    # extract columns to create time table
    time_table = log_df.select( col("dateTime").alias("start_time") \
                                 , hour("dateTime").alias('hour') \
                                 , dayofmonth("dateTime").alias('day') \
                                 , weekofyear("dateTime").alias('week') \
                                 , month("dateTime").alias('month') \
                                 , year("dateTime").alias('year') \
                                 , date_format("dateTime","E").alias('weekday') 
                               )
    #create temporary view for querying in sql
    log_df.createOrReplaceTempView("log_table")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode('overwrite').parquet(output_data+"time.parquet")

    # join songs table and song plays table to get a dataframe for the songplays table with relevant columns
    songplays_df = spark.sql(""" SELECT   dateTime AS start_time
                                        , userId
                                        , level 
                                        , song_id
                                        , artist_id
                                        , sessionId
                                        , location
                                        , userAgent
                                        , year(dateTime) AS year
                                        , month(dateTime) AS month
                                FROM log_table
                                JOIN songs_table ON songs_table.title = log_table.song 
                                WHERE dateTime IS NOT NULL
                                """)
    # write songplays table to parquet files partitioned by year and month
    songplays_df.write.partitionBy("year","month").mode('overwrite').parquet(output_data+"songplays.parquet")


def main():
    '''main function to call the above functions and provide s3 bucket name'''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    print(" processing song files.... ")
    process_song_data(spark, input_data, output_data)  
    
    print(" processing song files complete! processing log files..... ")
    process_log_data(spark, input_data, output_data)
    
    print("processing log files complete! ")

if __name__ == "__main__":
    main()
