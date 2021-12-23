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

    #Equivalently all this can be written in a repulsive sql query w/ the following
    final_df = spark.sql("""
        SELECT a.station, a.date, a.range/10 AS range
            FROM (SELECT tmax.station, tmax.date, tmax.value - tmin.value AS range 
                FROM (SELECT station, date, value FROM (SELECT * FROM d WHERE qflag IS NULL) WHERE observation = 'TMAX' ) AS tmax 
                INNER JOIN 
                (SELECT station, date, value FROM (SELECT * FROM d WHERE qflag IS NULL) WHERE observation = 'TMIN') AS tmin 
                ON tmin.date=tmax.date AND tmin.station=tmax.station) AS a 
            INNER JOIN 
                (SELECT date, max(range) AS m_range 
                    FROM (SELECT tmax.station, tmax.date, tmax.value - tmin.value AS range 
                        FROM (SELECT station, date, value 
                            FROM (SELECT * FROM d WHERE qflag IS NULL) WHERE observation = 'TMAX' ) AS tmax 
                            INNER JOIN 
                            (SELECT station, date, value 
                            FROM (SELECT * FROM d WHERE qflag IS NULL) WHERE observation = 'TMIN') AS tmin 
                        ON tmin.date=tmax.date AND tmin.station=tmax.station) 
                    GROUP BY date) AS b 
                ON a.date=b.date AND a.range=b.m_range ORDER BY a.date""")
        
    final_df.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)