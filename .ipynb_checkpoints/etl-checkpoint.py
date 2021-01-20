import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, FloatType, 


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function creates a spark session. This session will handle the entire data ETL
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function reads song json data from an S3 bucket located in the input_data path,
    transformns it using spark data transformation techniques 
    and writes it in the output_data S3 bucket path in a parquet format.
    """
    # get filepath to song data file
    song_data = config['S3']['SONG_DATA']
    
    # write a schema for the json file to have the correct data types onces read into a dataframe
    schema = StructType([
        StructField("num_songs", IntegerType()),
        StructField("artist_id", StringType()),
        StructField("artist_latitude", FloatType()),
        StructField("artist_longitude", FloatType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", FloatType()),
        StructField("year", IntegerType())])
    
    # read song data file
    df = spark.read.json(f"{input_data}{song_data}")

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy('artist_id', 'year').parquet(f"{output_data}songs/songs_table.parquet", mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              col('artist_name').alias('name'),
                              col('artist_location').alias('location'),
                              col('artist_latitude').alias('latitude'),
                              col('artist_longitude').alias('longitude')).dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table = artists_table.write.partitionBy('location').parquet(f"{output_data}artists/artists_table.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    This function reads log json file from an S3 bucket located in the input_data path,
    transformns it using spark data transformation techniques 
    and writes it in the output_data S3 bucket path in a parquet format.
    Then creates a business table using data from the song table previously created.
    """
    # get filepath to log data file
    log_data = config['S3']['LOG_DATA']
    
    # write a schema for the data to have the correct data types onces read
    schema = StructType([
        StructField("artist", StringType()),
        StructField("auth", StringType()),
        StructField("firstName", StringType()),
        StructField("gender", StringType()),
        StructField("itemInSession", IntegerType()),
        StructField("lastName", StringType()),
        StructField("length", FloatType()),
        StructField("level", StringType()),
        StructField("location", StringType()),
        StructField("method", StringType()),
        StructField("page", StringType()),
        StructField("registration", LongType()),
        StructField("sessionId", IntegerType()),
        StructField("song", StringType()),
        StructField("status", IntegerType()),
        StructField("ts", LongType()),
        StructField("userAgent", StringType()),
        StructField("userId", IntegerType())
    ])    

    # read log data file
    df = spark.read.json(f"{input_data}{log_data}")
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(col('userId').alias('user_id'),
                            col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'),
                            'gender',
                            'level').dropDuplicates(['user_id'])
    
    # write users table to parquet files
    users_table = users_table.write.partitionBy('gender', 'level').parquet(f"{output_data}users/users_table.parquet", mode="overwrite")

    # create timestamp column from original timestamp column. For this udf function to work, the correct datatype must be imported
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts)) 
    
    # create datetime column from original timestamp column. For this udf function to work, the correct datatype must be imported
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000), DateType())
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time'),
                           hour('datetime').alias('hour'),
                           dayofmonth('datetime').alias('day'),
                           weekofyear('datetime').alias('week'),
                           month('datetime').alias('month'),
                           year('datetime').alias('year'),
                           date_format('datetime','E').alias('weekday')) 
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy('year', 'month').parquet(f"{output_data}time/time_table.parquet", mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(f"{output_data}songs_table.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    from pyspark.sql.functions import monotonicallyIncreasingId

    songplays_table = df.select('song',
                                col('timestamp').alias('start_time'),
                                col('userId').alias('user_id'),
                                'level',
                                col('sessionId').alias('session_id'),
                                'location',
                                col('userAgent').alias('user_agent'))
    
    songplays_table = songplays_table.join(songs_df, songplays.song == songs_df.title)
    
    songplays_table = songplays_table.withColumn("songplay_id", monotonicallyIncreasingId())
    
    songplays_table = songplays_table.select('songplay_id'
                                             'start_time',
                                             'user_id',
                                             'level',
                                             'song_id',
                                             'artist_id',
                                             'session_id',
                                             'location',
                                             'user_agent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy('year', 'month').parquet(f"{output_data}songplays/songplays_table.parquet", mode="overwrite")


def main():
    """
    Executes the whole program in order,
    first it creates the spark session, then runs the song and log functions.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://alex-udacity-dlp-test/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
