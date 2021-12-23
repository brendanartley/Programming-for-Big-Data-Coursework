## Questions 

Hadoop's Mapreduce more like reduce or reducebykey. I am kind of confused?

## MapReduce Notes

Compilation on Cluster
    javac -classpath `hadoop classpath` PageView.java
    jar cf pageview.jar *.class

Mapper Test Run
    yarn jar pageview.jar PageView -D mapreduce.job.reduces=0 /courses/732
    pagecounts-0 output-1
    Note: Use pagecounts-0 when running tests as it is a 

In Browser Java Environment
    https://replit.com/languages/java10

Nifty Trick to see top 5 most seen pages
    hdfs dfs -cat ./output-0/part-* | sort -n -k4 -r | head -n5
    (sort -n -k4 -r) - "sort numerically by column #4, in descending order"
    (head -n5) - show top 5

In /courses/732/pagecounts-with-time-1, simon_pegg should be first with 6195

## Spark Notes

Spark variables:
    export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-15.0.1.jdk/Contents/Home
    export SPARK_HOME=/home/you/spark-3.1.2-bin-hadoop3.2/
    export PYSPARK_PYTHON=python3
    
## Answers 

1. I would implement this using a custom pairWritable class similar to what was given to us in assigment 1. This would be in the form (IntWritable, Text) and the page name would correspond to the Text object, and the page views would be the intwritable. 

Using this custom writable pair we would be able to view both the page name and number of views of each page that goes through the mapper and also keep track of the time-stamp. As a result, we would be able to find the name of the page that is most popular.

2. Map and Flatmap are different. The map function takes in an RDD of a specific length, and outputs an RDD with the same length. On the other hand, the flatmap function takes in an RDD of a certain length and produces a single RDD as a result.

In our pyspark wordcount example, the flatmap function performs similarly to hadoop's mapreduce. The mapper takes in each line of the text file as the input, and outputs multiple key-value pairs after we split and tokenize each word on the line. 

3. The reducebykey function is more like the Mapreduce concept of reducing, where each unique key is reduced to one key-value pair. The reduce function is different because it reduces the entire dataset to one final value. The reduce function could be helpful in finding something like the min/max of an RDD, whereas the reducebykey function would be used for getting "unique-key statistics."

4. Since we are passing a tuple with a tuple as the value during the reduceByKey stage, we can add a conditional statement that checks wether the view_counts are equal. In this case we can add the page names (strings) together with a "," to make sure we are not missing any pages with counts equal to the max.

The function passed to reduceByKey would look something like this.

def get_max(x, y):
    if x[0]>y[0]:
        return x
    elif x[0] == y[0]:
        return (x[0], x[1] + ", " + y[1])
    else:
        return y