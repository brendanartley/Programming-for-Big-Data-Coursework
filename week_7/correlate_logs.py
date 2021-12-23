import sys
import re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types #type:ignore

def main(inputs):

    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

    def split_line(line):
        return_split = line_re.split(line)
        if len(return_split) == 6:
            return (return_split[1], int(return_split[4]))
        return None

    logs_schema = types.StructType([
        types.StructField('host', types.StringType()),
        types.StructField('bytes', types.LongType()),
    ])

    rows = sc.textFile(inputs).map(split_line).filter(lambda x: x!=None)

    #host + bytes
    df = spark.createDataFrame(rows,schema=logs_schema)
    xy_df = df.groupBy(df['host']).agg(functions.count('bytes').alias('x'), functions.sum('bytes').alias('y'))

    #creating variables
    var_df = xy_df.withColumn('one', functions.lit(1)).withColumn('x2', xy_df['x']**2).withColumn('y2', xy_df['y']**2).withColumn('xy', xy_df['x']*xy_df['y'])
    sums_df = var_df.agg(functions.sum('one'), functions.sum('x'), functions.sum('y'), functions.sum('x2'), functions.sum('y2'), functions.sum('xy'))

    #collecting a small df with 6 cells 
    sums_dict = sums_df.collect()[0].asDict()

    #output for debugging
    for key in sums_dict:
        print(key, sums_dict[key])

    numerator = ((sums_dict['sum(one)'] * sums_dict['sum(xy)']) - (sums_dict['sum(x)'] * sums_dict['sum(y)']))
    denominator  = ((sums_dict['sum(one)'] * (sums_dict['sum(x2)']) - (sums_dict['sum(x)']**2)))**(1/2) * ((sums_dict['sum(one)'] * (sums_dict['sum(y2)']) - (sums_dict['sum(y)']**2)))**(1/2)
    
    #print R values
    print("\nR = {}".format(numerator/denominator))
    print("R^2 = {}".format((numerator/denominator)**2))

if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('clogs').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)