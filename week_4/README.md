### Questions

### Notes

### Answers

1. This actually made a significant difference in the overall execution time. When adding in .cache() to store the RDD in memory in the executor process, the code ran 21% faster. To be more specific the total real runtime with .cache() was ~42 seconds, and the total real runtime without .cache() was ~51 seconds.

Here are the results from timing on the cluster using reddit-4 and limiting myself to 8 executor processes. 

    ie --conf spark.dynamicAllocation.enabled=false --num-executors=8

    - Using .cache()
    real    0m42.462s
    user    0m19.594s
    sys     0m1.728s

    - Not using .cache()
    real    0m51.879s
    user    0m20.518s
    sys     0m1.527s

2. .cache() would make code slower if we are using it without reason. For example, if we are only performing one transformation on an RDD and then never performing any more transformations on that dataset again, then there is no point in using .cache() regardless of the dataset size. If we need to access the same dataset and perform multiple transformations on it, then using cache would be in our interest. Furthermore, we would run into trouble when using .cache() on a data frame that is very large, or if we are trying to cache() a large number of objects as .cache() forces spark to materialize the object we call upon, and we only want to do this if it makes sense in our program.

3. Broadcast Join works well when the broadcasted data is small, and the data being joined with is large. This is because broadcast join significantly reduces the amount of replicated data being sent across the network. In the relative_score case when we are joining a small RDD against the large comment data RDD, using a standard join operation will result in large data exchange between executors. We use a broadcast object to send a python dictionary to each executor and with that, all the data needed is available within each executor with little data exchanged/and relatively few duplicates of the data. If we did not use broadcast join we would have been sending many copies of the Reddit averages dataset across the network (and this adds up!).

Due to the limitations that we set in the assignment, using broadcast join simply allows our program to run by not exceeding processing capabilities. In the real world when we are using AWS/GCP the program might not fail as more resources would be allocated for use but the cost of obtaining the result will be much higher (assuming we are have enabled dynamic resource allocation).

4. Broadcast join will be slower when we are working with a dataset of unique keys or when the broadcasted dataset is quite large. In these cases, the amount of data that needs to be exchanged across the network slows down the overall runtime of our application and could result in broadcast join being slower than the traditional join operation.