## Questions 

- Having trouble acessing localhost:4040 during job on cluster?
    - not possible unless on computer lab computer

- java.lang.OutOfMemoryError: Java Heap Space error?
    - Use generators/iterators.


## Notes

Add the following at the end of line to ignore errors on the line (pylance)
- #type:ignore

Using time before function call results in
    - Real time
        - Time from start to finish of the call

    - User time
        - Actual CPU time used in executing the process

    - Sys time: 
        - CPU time spent in system calls within the kernel
    
    User + Sys time is the total execution time of the program.

Remote Workstations(for PMP students)
    - https://www.sfu.ca/computing/about/support/csil/pmp-workstations.html

## Answers

1. Under the staging tab, the reduceByKey stage takes a ridiculous amount of time. Upon further inspection, we can see that the input size of the files varies quite a bit. The max file size is ~259mb and the executor it is sent to executes in 3.6 minutes, whereas the executor that received the 82kd file executes in 2 seconds.

This is a problem as most of the executors are inactive for the majority of the execution time, waiting for one executor to finish its task. Repartitioning was definitely worth it as the load was distributed more evenly among the executors on the cluster. This decreased the execution time of our program and ensured we were processing in parralell more efficiently.

2. There were not any noticeable differences for wordcount-3 because repartitioning RDD's is an expensive operation (PySpark docs even warns that we should avoid repartitioning when we can). If our file sizes are relatively evenly distributed, the total time it takes to repartition the data may not be worth it. 

Regardless, it is important to know the data we are working with and prepare it accordingly before utilizing PySpark. This will ensure we are efficiently using the cluster resources and distributing the work evenly among executors on the cluster.

3. We could modify the wordcount-5 input prior to creating an application in Pyspark.
The maximum file size is ~259MB and the minimum file size is 82KB, and the other files are not evenly distributed. If we make sure that the text file sizes are of equal size prior to running a job, we can remove the repartitioning step, and reduce the application time even further.

It's important to note that this may not be possible in all real-world computing situations, and it all depends on the data/application you are working with. 

4. When testing with different numbers of partitions, I found that there was a relatively small change when the number of partitiions was kept below 100. Once I started going beyond this into 4 and 5 digit numbers, the difference started to get larger.

I have included the results of changing the partition value below. Note that the second argument when calling the function is the number of partitions in this version of the code. 

NOTE: I did not include all test results here.

    time spark-submit euler.py 1000000

    1 - 6.99s user 0.68s system 163% cpu 4.696 total
    5 - 7.43s user 0.75s system 183% cpu 4.452 total
    10 - 7.67s user 0.79s system 186% cpu 4.536 total
    100 - 8.24s user 0.84s system 199% cpu 4.542 total
    500 - 12.72s user 1.55s system 198% cpu 7.193 total
    1000 - 15.24s user 1.70s system 278% cpu 6.079 total
    10000 - 53.10s user 14.78s system 325% cpu 20.859 total

5. Spark definately seems to add some overhead to a job. The pypy implementation really sped up the application run-time by about 50%. I would be curious to see a timing comparison on a large scale application, or maybe if it is possible that an application ran with pypy is not as quick? The following results of tests are all run on a computer in the lab workstation. 


    # Standard CPython Implementation:
    real 0m43.750s
    user 0m12.751s
    sys  0m0.675s

    # Spark Python with Pypy:
    real 0m22.928s
    user 0m14.441s
    sys  0m0.707s
    
    # Non-Spark single-threaded PyPy:
    real 0m47.188s
    user 0m47.000s
    sys  0m0.025s

    # Non-Spark single-threaded C:
    gcc -Wall -O2 -o euler euler.c && time ./euler 1000000000

    real 0m24.762s
    user 0m24.761s
    sys 0m0.000s

