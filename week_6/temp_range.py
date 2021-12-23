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

    #read and filter (qflag, observation)
    df = spark.read.csv(inputs, schema = observation_schema)
    df = df.where(df['qflag'].isNull()).cache()

    #max, min, ranges
    tmax_df = df.filter(df['observation'] == functions.lit('TMAX')).withColumnRenamed('value','max_value')
    tmin_df = df.filter(df['observation'] == functions.lit('TMIN')).withColumnRenamed('value','min_value')

    #ranges, max_ranges
    range_df = tmax_df.join(tmin_df, ((tmax_df['date'] == tmin_df['date']) & (tmax_df['station'] == tmin_df['station']))).select(tmax_df['date'], tmax_df['station'], (tmax_df['max_value'] - tmin_df['min_value']).alias('range')).cache()
    max_range_df = range_df.groupby(range_df['date']).agg(functions.max('range')).withColumnRenamed('date','date1')
    
    #output
    final_df = range_df.join(max_range_df, ((range_df['date'] == max_range_df['date1']) & (range_df['range'] == max_range_df['max(range)']))).select(range_df['date'], range_df['station'], range_df['range']/10).orderBy(range_df['date'])
    final_df.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)