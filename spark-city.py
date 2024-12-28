from pyspark.sql import SparkSession
from jobs.config import configuration
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col, lit

def validate_vehicle_data(df):
    """Validate vehicle data schema."""
    return df.filter(
        (col('id').isNotNull()) &
        (col('timestamp').isNotNull()) &
        (col('location').isNotNull()) &
        (col('speed').isNotNull()) &
        (col('direction').isNotNull()) &
        (col('make').isNotNull()) &
        (col('model').isNotNull()) &
        (col('year').isNotNull()) &
        (col('fuelType').isNotNull())
    ).withColumn("validation_error", lit(None))  # Add a column to log validation status
    
def handle_invalid_data(df, output_path):
    """Handle invalid data by storing it in a separate location for debugging."""
    invalid_data = df.filter(
        (col('id').isNull()) |
        (col('timestamp').isNull()) |
        (col('location').isNull()) |
        (col('speed').isNull()) |
        (col('direction').isNull()) |
        (col('make').isNull()) |
        (col('model').isNull()) |
        (col('year').isNull()) |
        (col('fuelType').isNull())
    )
    invalid_data.write.format("parquet").mode("append").save(output_path)



def main():
    spark = SparkSession.builder.appName('SmartCityStreaming') \
        .config('spark.jars.packages',
                'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,'
                'org.apache.hadoop:hadoop-aws:3.3.1,'
                'com.amazonaws:aws-java-sdk:1.11.469') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.hadoop.fs.s3a.access.key', configuration["AWS_ACCESS_KEY"]) \
        .config('spark.hadoop.fs.s3a.secret.key', configuration["AWS_SECRET_KEY"]) \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()

    ## Adjust the log level to minimize the output
    spark.sparkContext.setLogLevel('WARN')

    ## Vehicle Schema
    vehicleSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),  # Corrected field name
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),  # Assuming location is a string
        StructField('speed', DoubleType(), True),
        StructField('direction', StringType(), True),
        StructField('make', StringType(), True),
        StructField('model', StringType(), True),
        StructField('year', IntegerType(), True),
        StructField('fuelType', StringType(), True),  # Corrected field name
    ])

    ## GPS Schema
    gpsSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),  # Corrected field name
        StructField('timestamp', TimestampType(), True),
        StructField('speed', DoubleType(), True),
        StructField('direction', StringType(), True),
        StructField('vehicle_type', StringType(), True)  # Corrected field name
    ])

    ## Traffic Schema
    trafficSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),  # Corrected field name
        StructField('cameraId', StringType(), True),
        StructField('location', StringType(), True),  # Assuming location is a string
        StructField('timestamp', TimestampType(), True),
        StructField('snapshot', StringType(), True),
    ])

    ## Weather Schema
    weatherSchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),  # Corrected field name
        StructField('location', StringType(), True),  # Assuming location is a string
        StructField('timestamp', TimestampType(), True),
        StructField('temperature', DoubleType(), True),
        StructField('weatherCondition', StringType(), True),  # Corrected type
        StructField('precipitation', DoubleType(), True),
        StructField('windspeed', DoubleType(), True),
        StructField('humidity', IntegerType(), True),
        StructField('airQualityIndex', DoubleType(), True),
    ])

    ## Emergency Data Schema
    emergencySchema = StructType([
        StructField('id', StringType(), True),
        StructField('deviceId', StringType(), True),  # Corrected field name
        StructField('incidentId', StringType(), True),
        StructField('type', StringType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('location', StringType(), True),  # Assuming location is a string
        StructField('status', StringType(), True),
        StructField('description', StringType(), True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream.
                format('kafka').
                option('kafka.bootstrap.servers', configuration["KAFKA_BOOTSTRAP_SERVERS"]).
                option('subscribe', topic).
                option('startingOffsets', 'earliest').
                load().
                selectExpr('CAST(value AS STRING)').
                select(from_json(col('value'), schema).alias('data')).
                select('data.*').
                withWatermark('timestamp', '2 minutes')
                )

    def streamWriter(input, checkpointFolder, output):
        return (input.writeStream.format('parquet').option('checkpointLocation', checkpointFolder) \
                .option('path', output).outputMode('append').start())

    vehicleDF = read_kafka_topic(topic=configuration["VEHICLE_TOPIC"], schema=vehicleSchema).alias('vehicle')

    # Validate vehicle data and handle invalid entries
    valid_vehicle_data = validate_vehicle_data(vehicleDF)
    handle_invalid_data(vehicleDF, "s3a://smart-city-debug/invalid_vehicle_data")

    gpsDF = read_kafka_topic(topic=configuration["GPS_TOPIC"], schema=gpsSchema).alias('gps')
    trafficDF = read_kafka_topic(topic=configuration["TRAFFIC_TOPIC"], schema=trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic(topic=configuration["WEATHER_TOPIC"], schema=weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic(topic=configuration["EMERGENCY_TOPIC"], schema=emergencySchema).alias('emergency')

    q1 = streamWriter(
        input=valid_vehicle_data,
        checkpointFolder='s3a://smart-city-data/checkpoints/vehicle_data',
        output='s3a://smart-city-data/vehicle_data')
    q2 = streamWriter(input=gpsDF,
                      checkpointFolder='s3a://spark-streaming-data-smart-city-project/checkpoints/gps_data',
                      output='s3a://spark-streaming-data-smart-city-project/data/gps_data')
    q3 = streamWriter(input=trafficDF,
                      checkpointFolder='s3a://spark-streaming-data-smart-city-project/checkpoints/traffic_data',
                      output='s3a://spark-streaming-data-smart-city-project/data/traffic_data')
    q4 = streamWriter(input=weatherDF,
                      checkpointFolder='s3a://spark-streaming-data-smart-city-project/checkpoints/weather_data',
                      output='s3a://spark-streaming-data-smart-city-project/data/weather_data')
    q5 = streamWriter(input=emergencyDF,
                      checkpointFolder='s3a://spark-streaming-data-smart-city-project/checkpoints/emergency_data',
                      output='s3a://spark-streaming-data-smart-city-project/data/emergency_data')
    q5.awaitTermination()


if __name__ == '__main__':
    main()
