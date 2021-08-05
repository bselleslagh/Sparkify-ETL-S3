#import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType as Ts

'''
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']
'''


def create_spark_session():
    '''
    Start a Spark sessions with the Hadoop packages installed
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Given the input and output paths on S3, this function will read all song data stored in JSON-format,
    transform this data song and artists tables and store them on S3 as .parquet files.
    '''    
    # get filepath to song data file
    song_data = 's3a://udacity-dend/song_data/*/*/*/*.json'
    
    # read song data file
    song_schema = R([
    Fld("num_songs", Int()),
    Fld("artist_id", Str()),
    Fld("artist_latitude", Dbl()),
    Fld("artist_longitude", Dbl()),
    Fld("artist_location", Str()),
    Fld("artist_name", Str()),
    Fld("song_id", Str()),
    Fld("title", Str()),
    Fld("duration", Dbl()),
    Fld("year", Int())
    ])
    
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet("%ssongs.parquet" % output_data)

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # rename the columns
    artists_table = artists_table.withColumnRenamed("artist_name", "name") \
    .withColumnRenamed("artist_location", "location") \
    .withColumnRenamed("artist_latitude", "latitude") \
    .withColumnRenamed("artist_longitude", "longitude")
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet("%sartists.parquet" % output_data)


def process_log_data(spark, input_data, output_data):
    '''
    Given input and output paths on S3, this function will load the stored log data and transform this into the following tables:
    users, time_table and songplays
    '''
    # get filepath to log data file
    log_data = '%slog_data/*/*/*.json' % input_data

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table    
    users = df.select("userId", "firstName", "lastName", "gender", "level")
    users = users.withColumnRenamed("userId", "user_id") \
    .withColumnRenamed("firstName", "first_name")\
    .withColumnRenamed("lastName", "last_name")

    # write users table to parquet files
    users.write.mode("overwrite").parquet("%susers.parquet" % output_data)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x/1000.0)), Ts())
    df = df.withColumn("start_time", get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x/1000)), Ts())
    df = df.withColumn("datetime", get_datetime('ts'))
        
    # extract columns to create time table
    time_table = df.select('start_time')\
    .withColumn('hour', hour('start_time'))\
    .withColumn('day', dayofmonth('start_time'))\
    .withColumn('week', weekofyear('start_time'))\
    .withColumn('month', month('start_time'))\
    .withColumn('year', year('start_time'))\
    .withColumn('weekday', dayofweek('start_time'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet("%stime_table.parquet" % output_data)

    # read in song data to use for songplays table
    song_df = spark.read.parquet("%ssongs.parquet" % output_data)  
    song_df.createOrReplaceTempView("songs")
    df.createOrReplaceTempView("songplays")
    
    songplays_query = '''
    SELECT a.start_time, a.userId, a.level, b.song_id, b.artist_id, a.sessionId, a.location, a.userAgent, 
    YEAR(a.start_time) as year, 
    MONTH(a.start_time) as month
    FROM songplays a
    LEFT JOIN songs b ON a.song = b.title
    '''
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(songplays_query)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy('year', 'month').parquet("%ssongplays.parquet" % output_data)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://bselleslagh/sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
