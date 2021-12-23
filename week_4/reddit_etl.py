from pyspark import SparkConf, SparkContext #type:ignore 
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json

def main(inputs, output):

    def load_json(x):
        return json.loads(x)

    def subr_score_author(x):
        return {"subreddit": x["subreddit"], "score": x["score"], "author": x["author"]}

    def contains_e(x):
        return "e" in x["subreddit"]

    def output_format(x):
        return json.dumps(x)

    text = sc.textFile(inputs)
    r_json = text.map(load_json).map(subr_score_author).filter(contains_e).cache()

    pos = r_json.filter(lambda x: x["score"] > 0).map(output_format)
    neg = r_json.filter(lambda x: x["score"] <= 0).map(output_format)

    pos.saveAsTextFile(output + '/positive')
    neg.saveAsTextFile(output + '/negative')

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit_etl')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)