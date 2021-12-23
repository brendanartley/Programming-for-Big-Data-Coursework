import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions


def main(inputs):
    SEED = 10
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.80, 0.20], seed=SEED)
    train = train.cache()
    validation = validation.cache()
    
    # TODO: create a pipeline to predict RGB colours -> word
    stringIndexer = StringIndexer(inputCol="word", outputCol="label", handleInvalid="error", stringOrderType="frequencyDesc")
    assemble_features = VectorAssembler(inputCols=["R","G","B"], outputCol='features')
    rgb_classifier = MultilayerPerceptronClassifier(layers=[3, 180, 11], seed=SEED, maxIter=100)
    
    rgb_pipeline = Pipeline(stages=[stringIndexer, assemble_features, rgb_classifier])
    
    # TODO: create an evaluator and score the validation data
    rgb_model = rgb_pipeline.fit(train)
    rgb_predictions = rgb_model.transform(validation)

    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    rgb_score = evaluator.evaluate(rgb_predictions)

    plot_predictions(rgb_model, 'RGB', labelCol='word')
    print('Validation score for RGB model: ', rgb_score)
    
    # TODO: create a pipeline RGB colours -> LAB colours -> word; train and evaluate.

    rgb_to_lab_query = SQLTransformer(statement=rgb2lab_query(passthrough_columns=['R', 'G', 'B', 'word']))
    lab_assemble_features = VectorAssembler(inputCols=['labL', 'labA', 'labB'], outputCol='features')
    lab_classifier = MultilayerPerceptronClassifier(layers=[3, 100, 11], seed=SEED, maxIter=150)

    lab_pipeline = Pipeline(stages=[rgb_to_lab_query, stringIndexer, lab_assemble_features, lab_classifier])

    lab_model = lab_pipeline.fit(train)
    lab_predictions = lab_model.transform(validation)
    lab_score = evaluator.evaluate(lab_predictions)

    plot_predictions(lab_model, 'LAB', labelCol='word')
    print('Validation score for LAB model: ', lab_score)

    
if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)