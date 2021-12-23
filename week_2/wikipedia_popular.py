from pyspark import SparkConf, SparkContext
import sys
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('test')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

#example line - (20160801-020000 en AaagHiAag 1 8979)

#Really helpful documentation on RDD's
#https://spark.apache.org/docs/latest/rdd-programming-guide.html
#Python types -> Writables (Under External Datasets/Python)

def split_line(line):
    return (x for x in line.split())

def views_to_int(abcde):
    a,b,c,d,e = abcde
    return (a,b,c,int(d),e)

def bad_records(abcde):
    a,b,c,d,e = abcde
    if (b != "en") or (c == "Main_Page") or (c.startswith("Special:")):
        return False
    return True

def create_key_vals(abcde):
    a,b,c,d,e = abcde
    return (a, (d,c))

def get_max(x, y):
    return x if x[0]>y[0] else y

def get_key(kv):
    return kv[0]

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])

text = sc.textFile(inputs)
pages = text.map(split_line).map(views_to_int).filter(bad_records).map(create_key_vals)

top_pages = pages.reduceByKey(get_max)
outdata = top_pages.sortBy(get_key).map(tab_separated).saveAsTextFile(output)
