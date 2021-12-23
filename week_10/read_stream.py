import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('streaming example').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')


def main(topic):

    #stream data
    stream_lines = (spark.readStream.format('kafka')
            .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092')
            .option('subscribe', topic).load())

    lines = stream_lines.select(stream_lines['value'].cast('string'))

    #preprocessing input
    vals = (lines.withColumn('x', functions.split(lines['value'], ' ').getItem(0))
                    .withColumn('x2', functions.split(lines['value'], ' ').getItem(0) ** 2)
                    .withColumn('y', functions.split(lines['value'], ' ').getItem(1))
                    .withColumn('xy', functions.split(lines['value'], ' ').getItem(0) * functions.split(lines['value'], ' ').getItem(1)))

    #sum columns
    av = vals.select(functions.sum("x").alias("x"), 
                           functions.sum("y").alias("y"), 
                           functions.sum("x2").alias("x2"), 
                           functions.sum("xy").alias("xy"),
                           functions.count("x").alias("n")
                           )

    #calculating slope and intercept
    final = av.select((((av["xy"]) - ((1/av["n"]) * (av["x"] * av["y"]))) / (av["x2"] - ((1/av["n"]) * av["x"] * av["x"]))).alias("Slope"), 
                         ((av["y"]/av["n"]) - ((((av["xy"]) - ((1/av["n"]) * (av["x"] * av["y"]))) / (av["x2"] - ((1/av["n"]) * av["x"] * av["x"]))) * (av["x"]/av["n"]))).alias("Intercept"))

    stream = final.writeStream.format('console') \
            .outputMode('complete').start()
    stream.awaitTermination(600)

if __name__ == '__main__':
    topic = sys.argv[1]
    main(topic)