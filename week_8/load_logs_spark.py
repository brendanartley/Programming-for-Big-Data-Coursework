from datetime import datetime
import sys, re, uuid
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

def main(input_dir, keyspace, table):

    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

    def split_line(line):
        return_split = line_re.split(line)
        if len(return_split) == 6:
            dt = datetime.strptime(return_split[2], '%d/%b/%Y:%H:%M:%S')
            return (return_split[1], dt, return_split[3], int(return_split[4]), str(uuid.uuid4()))
        return None

    logs_schema = types.StructType([
        types.StructField('host', types.StringType()),
        types.StructField('datetime', types.TimestampType()),
        types.StructField('path', types.StringType()),
        types.StructField('bytes', types.LongType()),
        types.StructField('uniq_id', types.StringType()),
        ])

    rows = sc.textFile(input_dir).map(split_line).filter(lambda x: x!=None)
    df = spark.createDataFrame(rows, schema=logs_schema).repartition(32)

    df.write.format("org.apache.spark.sql.cassandra").mode("Append").options(table=table, keyspace=keyspace).save()

if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]

    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('cassandra').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_dir, keyspace, table)