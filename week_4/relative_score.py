from pyspark import SparkConf, SparkContext #type:ignore
import sys
import json

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def main(inputs, output):

    def load_json(x):
        return json.loads(x)

    def to_key_val(x):
        return (x['subreddit'], (x['score'], 1))

    def get_key(kv):
        return kv[0]

    def get_avg(abc):
        a,(b,c) = abc
        return (a, b/c)

    def add(x, y):
        x1, x2 = x
        y1, y2 = y
        return (x1+y1, x2+y2)
    
    def output_format(kv):
        return json.dumps([kv[0], kv[1]])

    def no_negatives(kv):
        k, v = kv
        return v >= 0

    def get_relative_score(ksd):
        k, (s, d) = ksd
        return (d['score']/s, d['author'])

    #getting subreddit avgs
    comment_data = sc.textFile(inputs).map(load_json).cache()
    subreddit_avgs = comment_data.map(to_key_val).reduceByKey(add)
    only_positive = subreddit_avgs.map(get_avg).filter(no_negatives).sortBy(get_key)

    #getting best relative scores
    pairs = comment_data.map(lambda x: (x['subreddit'], x))
    relative_scores = only_positive.join(pairs).map(get_relative_score).sortBy(get_key, ascending = False)
    
    #getting output data
    outdata = relative_scores.map(output_format)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('relative_score')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)