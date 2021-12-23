from pyspark import SparkConf, SparkContext
import sys, random
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
#random.random() - generates random float between 0-1

def main(sample_count):

    def get_e_count(x):
        random.seed()
        c = 0
        for i in range(x):
            i = 0
            while i < 1:
                i += random.random()
                c += 1
        return c

    def get_length(x):
        return len(x)

    def add(a,b):
        return a + b

    slice_count = 10
    partition_size = sc.range(sample_count, numSlices = slice_count).glom().map(get_length)

    e_total = partition_size.map(get_e_count).reduce(add)
    print(e_total/sample_count)

if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    sample_count = int(sys.argv[1])
    main(sample_count)