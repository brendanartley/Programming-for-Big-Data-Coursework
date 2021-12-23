from pyspark import SparkConf, SparkContext #type:ignore
import sys
import string, re

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def main(inputs, output):

    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

    def words_once(line):
        for w in line.split():
            for val in wordsep.split(w):
                yield (val.lower(), 1)
                    
    def add(x, y):
        return x + y

    def get_key(kv):
        return kv[0]

    def output_format(kv):
        k, v = kv
        return '%s %i' % (k, v)

    text = sc.textFile(inputs).repartition(8)
    words = text.flatMap(words_once).filter(lambda x: x[0] != "")
    wordcount = words.reduceByKey(add)

    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('wc_improved')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)