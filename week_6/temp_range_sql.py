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

    #reading and caching df
    d = spark.read.csv(inputs, schema = observation_schema).createOrReplaceTempView('d')
    df = spark.sql("""SELECT * FROM d WHERE qflag IS NULL""").cache()
    df.createOrReplaceTempView('df')

    #tmax and tmin
    max_df = spark.sql("""SELECT station, date, value FROM df WHERE observation = 'TMAX' """).createOrReplaceTempView('max_df')
    min_df = spark.sql("""SELECT station, date, value FROM df WHERE observation = 'TMIN' """).createOrReplaceTempView('min_df')

    #range / cache range / max range
    range_df = spark.sql("""SELECT max_df.station, max_df.date, max_df.value - min_df.value AS range FROM max_df INNER JOIN min_df ON min_df.date=max_df.date AND min_df.station=max_df.station""").cache()
    range_df.createOrReplaceTempView('range_df')
    max_range_df = spark.sql("""SELECT date, MAX(range) AS m_range FROM range_df GROUP BY date """).createOrReplaceTempView('max_range_df')

    #output df
    final_df = spark.sql("""SELECT range_df.date, range_df.station, range_df.range/10 AS range FROM range_df INNER JOIN max_range_df ON range_df.date=max_range_df.date AND range_df.range=max_range_df.m_range ORDER BY range_df.date""")
    final_df.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)