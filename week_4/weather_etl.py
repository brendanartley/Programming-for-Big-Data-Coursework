import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types #type:ignore

# add more functions as necessary

def main(inputs, output):

    observation_schema = types.StructType([
        types.StructField('station', types.StringType()),
        types.StructField('date', types.StringType()),
        types.StructField('observation', types.StringType()),
        types.StructField('value', types.IntegerType()),
        types.StructField('mflag', types.StringType()),
        types.StructField('qflag', types.StringType()),
        types.StructField('sflag', types.StringType()),
        types.StructField('obstime', types.StringType()),
    ])

    #extract, transform, load
    weather = spark.read.csv(inputs, schema=observation_schema)
    w_filtered = weather.where(weather['mflag'].isNull()).filter(weather['station'].startswith('CA')).where(weather['observation'] == 'TMAX')
    w_tmax = w_filtered.withColumn('tmax', w_filtered['value']/10)
    w_final = w_tmax.select(w_tmax['station'], w_tmax['date'], w_tmax['tmax'])

    #output
    w_final.write.json(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('weather_etl').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)