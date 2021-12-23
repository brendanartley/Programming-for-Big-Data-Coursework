from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def main(inputs, output, source, destination):

    def get_edges(line):
        arr = line.strip().split(" ")
        arr[0] = arr[0][:-1]
        if len(arr[1:])!=0:
            arr = [int(x) for x in arr]
            yield (arr[0], arr[1:])

    def get_next_step(nrd, val):
        n, (r, d) = nrd
        return d == val

    def to_path(vt, d):
        v, t = vt
        for l in t:
            if type(l) == list:
                for val in l:
                    yield(val, (v, d))
    
    def get_shortest(ab, xy):
        a, b = ab
        x, y = xy
        if b < y:
            return ab
        return xy

    edge_rdd =  sc.textFile(inputs).flatMap(get_edges).cache()
    path_rdd = sc.parallelize([(source,(-1, 0))]).cache()
    found = False
    
    #take steps
    for i in range(6):
        new_rdd = edge_rdd.join(path_rdd.filter(lambda x, z=i: get_next_step(x, z))).flatMap(lambda x, z=i+1: to_path(x, z))
        path_rdd = path_rdd.union(new_rdd).reduceByKey(get_shortest).cache()
        path_rdd.saveAsTextFile(output + '/iter-' + str(i))

        if path_rdd.filter(lambda x: x[0] == destination).isEmpty() == False:
            found=True
            break

    #recreate path if found
    if found:
        final_path = [destination]
        for i in range(5):
            destination = path_rdd.lookup(destination)[0][0]
            if destination == -1:
                break
            else:
                final_path.append(destination)
        final_path = sc.parallelize(final_path[::-1])
        final_path.saveAsTextFile(output + "/path")
    else:
        final_path = sc.parallelize(["No path found"])
        final_path.saveAsTextFile(output + "/path")

if __name__ == '__main__':
    conf = SparkConf().setAppName('dykstra')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1] + '/links-simple-sorted.txt'
    output = sys.argv[2]
    source = int(sys.argv[3])
    destination = int(sys.argv[4])
    main(inputs, output, source, destination)