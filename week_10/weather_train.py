import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import SQLTransformer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline


tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def train_model(inputs, output):

    #read data
    SEED = 198
    data = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25], seed=SEED)
    train = train.cache()
    validation = validation.cache()

    #pipeline stages
    sql_trans = SQLTransformer(
        statement = '''SELECT 
                        today.date,
                        today.latitude, 
                        today.longitude,
                        today.elevation,
                        dayofyear(today.date) AS dayofyear,
                        yesterday.tmax AS yesterday_tmax,
                        today.tmax
                        FROM __THIS__ as today
                        INNER JOIN __THIS__ as yesterday
                            ON date_sub(today.date, 1) = yesterday.date
                            AND today.station = yesterday.station''')

    assemble_features = VectorAssembler(inputCols=["latitude","longitude", "elevation", "dayofyear", "yesterday_tmax"], outputCol='features')
    gbt_regressor = GBTRegressor(featuresCol="features", labelCol='tmax', maxIter=10, seed=SEED)
    tmax_pipeline = Pipeline(stages=[sql_trans, assemble_features, gbt_regressor])

    #fit / predict / evaluators
    tmax_model = tmax_pipeline.fit(train)
    tmax_predictions = tmax_model.transform(validation)

    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='rmse')
    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax', metricName='r2')

    # Validation RMSE
    rmse = rmse_evaluator.evaluate(tmax_predictions)
    r2 = r2_evaluator.evaluate(tmax_predictions)
    print("Validation RMSE: {}, R^2: {}".format(rmse, r2))

    # Train RMSE
    tmax_predictions2 = tmax_model.transform(train)
    rmse2 = rmse_evaluator.evaluate(tmax_predictions2)
    r22 = r2_evaluator.evaluate(tmax_predictions2)
    print("Train RMSE: {}, R^2: {}".format(rmse2, r22))

    #Model Save
    tmax_model.write().overwrite().save(output)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    train_model(inputs, output)