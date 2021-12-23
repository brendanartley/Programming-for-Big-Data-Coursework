import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
from datetime import datetime


tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def train_model(inputs):
    
    #manual creation of df
    pred_data = [('SFU', datetime.strptime('2012-11-12','%Y-%m-%d'), 49.2771, -122.9146, 330.0, 12.0),
                 ('SFU', datetime.strptime('2012-11-13','%Y-%m-%d'), 49.2771, -122.9146, 330.0, 0.0)
    ]
    pred_df = spark.createDataFrame(pred_data, tmax_schema)

    #load model + make prediction
    model = PipelineModel.load(inputs)
    prediction = model.transform(pred_df).collect()[0]["prediction"]

    print('Predicted tmax tomorrow:', prediction)

if __name__ == '__main__':
    inputs = sys.argv[1]
    train_model(inputs)
