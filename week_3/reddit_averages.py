from pyspark import SparkConf, SparkContext #type:ignore
import sys
import json

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary

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

    text = sc.textFile(inputs)
    pairs = text.map(load_json).map(to_key_val)
    subreddit_avgs = pairs.reduceByKey(add)
    
    outdata = subreddit_avgs.map(get_avg).sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit_avg')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)