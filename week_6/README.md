### Questions

### Notes


### Answers

### Answers

1. When looking at avg_df.explain() we can see on the following line that we only read in the subreddit and score information from the JSON data. "ReadSchema: struct<score:bigint,subreddit:string>" This is really handy that spark does this for us and ensures we are only reading in the necessary information to execute our application.

Spark does do a combiner-like step where it computes the partial_avg() which is a per-partition operation. This is indicated on the following line "+- *(1) HashAggregate(keys=[subreddit#613], functions=[partial_avg(score#611L)])". After this combiner-like step the exchange step happens where these partial averages are sent across the network, and then the final average for each subreddit is computed. We can see the exchange line as follows "+- Exchange hashpartitioning(subreddit#613, 200), ENSURE_REQUIREMENTS, [id=#163]". Finally we can see the final average being computed on the following line"*(2) HashAggregate(keys=[subreddit#613], functions=[avg(score#611L)])".

I have provided the full physical plan below.

   == Physical Plan ==
   *(2) HashAggregate(keys=[subreddit#613], functions=[avg(score#611L)])
   +- Exchange hashpartitioning(subreddit#613, 200), ENSURE_REQUIREMENTS, [id=#163]
      +- *(1) HashAggregate(keys=[subreddit#613], functions=[partial_avg(score#611L)])
         +- FileScan json [score#611L,subreddit#613] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex[file:/../reddit-1], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<score:bigint,subreddit:string>

2. Using several different implementations of calculating the average score by subreddit, we can see the different execution times. I have included the detailed real/user/sys times below. For DataFrames, I saw a 3% reduction in the code runtime when running with pypy. To be more specific the code ran in ~65 seconds w/ CPython and in ~63 seconds w/ pypy. In the case of RDD's there was a much larger difference between the different versions with a 64% reduction in real runtime when using pypy over CPython. Furthermore, the code using rdd's ran in ~153 seconds with CPython, and in ~55 seconds with pypy.

The reason that we see the execution time of RDD's decrease more than dataframes is because of what happens under the hood. RDD's are serialized python objects, and all the work being done occurs bypassing this serialized data back to python code. On the other hand, dataframes contain JVM objects, with all the manipulation being done in the JVM. This means that we are passing descriptions of the calculations to the JVM rather than the data itself. This is why we see a little decrease in the execution time for dataframes, and a significant decrease in the execution time of our RDD implementation when run with pypy.


   MapReduce
      real	1m31.242s
      user	0m6.535s
      sys	0m0.481s

   Spark DataFrames (with CPython)
      real	1m5.780s
      user	0m28.071s
      sys	0m1.878s 

   Spark RDDs (with CPython)
      real	2m33.745s
      user	0m23.332s
      sys	0m1.686s

   Spark DataFrames (with PyPy)
      real	1m3.704s
      user	0m27.103s
      sys	0m1.575s

   Spark RDDs (with PyPy)
      real	0m55.277s
      user	0m20.382s
      sys	0m1.419s

3. The broadcast hint made a significant difference. Running the Wikipedia popular code without a broadcast hint on pagecounts-3 resulted in an execution time of ~101 seconds. On the other hand, running the Wikipedia popular code with a broadcast hint resulted in an execution time of ~80 seconds. This is roughly a 20% reduction in runtime. As we saw in this example and also in last week's assignment, using broadcast join can speed up our applications significantly as we are limiting the amount of data being sent across the network and as a result running a more efficient application.

I have included the exact testing times of the Wikipedia code below.

No broadcast hint (pagecounts-3)
   real    1m41.688s
   user    0m37.637s
   sys     0m2.429s

With broadcast hint (pagecounts-3)
   real    1m20.413s
   user    0m34.497s
   sys     0m2.297s


4. The execution plan is different when we forced spark not to execute a broadcast join. In the case where we want the broadcast join to occur we see the following line. When we force spark not to use a broadcast join, it instead uses a SortMergeJoin. This is comprised of two steps, where each partition is sorted, and then the actual merge takes place. We can see each variation of the application in the following lines of the execution plans.

   
   With broadcast hint
      ... 
      +- *(3) BroadcastHashJoin [views#2L, hour#15], [max(views)#39L, hour#42], Inner, BuildRight, false
      ... 
   Without broadcast hint
      ... 
      +- *(6) SortMergeJoin [views#2L, hour#15], [max(views)#39L, hour#42], Inner
      :  - *(2) Sort [views#2L ASC NULLS FIRST, hour#15 ASC NULLS FIRST], false, 0
      :     +- Exchange hashpartitioning(views#2L, hour#15, 200), ENSURE_REQUIREMENTS, [id=#162]
      ... 

The lines that were shown above are the only lines that differ between the two applications .explain(), yet they are very important in terms of how they affect the overall optimization of the application. 

5. I found that writing in the “DataFrames + Python methods” style was much more preferable. First of all, when I was writing the code for the weather data question I found it very handy to use the pyspark shell to visualize some of the steps in real-time. Although this works for both the "dataframe" style and the "temp tables" style, debugging the stages of the application was much easier in the "dataframe" style.

This is because of how readable and intuitive the built-in data frame functions are in pyspark. With dataframes it seems very difficult to get stuck because of not understanding the code. This is different from "temp tables + SQL syntax." Although in this case, it is relatively easy to understand the queries being made, we can easily overstep the line and start having to write multiline queries. The ease at which we can chain dataframe operations and make them readable makes the dataframe method a no-brainer.

Consider trying to read a one-line implementation of the temp_range_sql below. I wrote this query using the SQL implementation yet struggle to even follow what's happening. This would not happen to the same degree in the "dataframe" style as it is much more readable.

    final_df = spark.sql("""
        SELECT a.station, a.date, a.range/10 AS range
            FROM (SELECT tmax.station, tmax.date, tmax.value - tmin.value AS range 
                FROM (SELECT station, date, value FROM (SELECT * FROM d WHERE qflag IS NULL) WHERE observation = 'TMAX' ) AS tmax 
                INNER JOIN 
                (SELECT station, date, value FROM (SELECT * FROM d WHERE qflag IS NULL) WHERE observation = 'TMIN') AS tmin 
                ON tmin.date=tmax.date AND tmin.station=tmax.station) AS a 
            INNER JOIN 
                (SELECT date, max(range) AS m_range 
                    FROM (SELECT tmax.station, tmax.date, tmax.value - tmin.value AS range 
                        FROM (SELECT station, date, value 
                            FROM (SELECT * FROM d WHERE qflag IS NULL) WHERE observation = 'TMAX' ) AS tmax 
                            INNER JOIN 
                            (SELECT station, date, value 
                            FROM (SELECT * FROM d WHERE qflag IS NULL) WHERE observation = 'TMIN') AS tmin 
                        ON tmin.date=tmax.date AND tmin.station=tmax.station) 
                    GROUP BY date) AS b 
                ON a.date=b.date AND a.range=b.m_range ORDER BY a.date""")