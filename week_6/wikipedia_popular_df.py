import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types #type:ignore

# add more functions as necessary

def main(inputs, output):

    @functions.udf(returnType=types.StringType())
    def path_to_hour(path):
        file_name = path.rsplit('/', 1)[-1]
        fn_no_extension = file_name.split('.')[0]
        return fn_no_extension.lstrip('pagecounts-')

    pages_schema = types.StructType([
        types.StructField('language', types.StringType()),
        types.StructField('title', types.StringType()),
        types.StructField('views', types.LongType()),
        types.StructField('size', types.LongType())])

    #read files w/ filename
    df = (spark.read.csv(inputs, sep=" ", schema = pages_schema)
            .withColumn('filename', functions.input_file_name()))

    #filter en, main_page, special
    f_df = (df.where(df['language'] == functions.lit('en'))
                .filter((~df['title'].like('Special:%')) & (df['title'] != functions.lit('Main_Page'))))
    
    #applying udf to filename and getting max_view counts
    udf_df = f_df.select(path_to_hour(f_df['filename']).alias('hour'), f_df['title'], f_df['views']).cache()
    max_df = udf_df.groupBy(udf_df['hour']).max()

    #joining dataframes and selecting only those equal to max
    j_df = (udf_df.join(max_df, on=udf_df['hour'] == max_df['hour'], how='inner')
            .where(max_df['max(views)'] == udf_df['views'])
            .select(udf_df['hour'], udf_df['title'], udf_df['views']))

    #output df
    j_df = j_df.orderBy(j_df['hour'])
    j_df.write.json(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('example code').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)