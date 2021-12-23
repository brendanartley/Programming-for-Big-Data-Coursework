### Questions

### Notes

There are a few more intermediary steps, but the two core processes in the WordCount.java example is the Map and Reducer phase.

- Map Phase
    - Splits the strings into key value pairs.
    - "An elephant is an animal"
        (an,1)
        (elephant,1)
        (is,1)
        (an,1)
        (animal,1)

- Reducer Phase
    - Three intermediary phases = shuffle, sort, reduce
    - Essentially all the key-value pairs generated in the map phase are sorted alphabetically, and key-value pairs with the same key are added together. This results in every distinct keys in with total counts in the output file. 

### Answers

1. Prior to Monday's lecture I was struggling to understand how the Mapper, Combiner, and Reducer work together to efficiently execute an application sent to the cluster. After Monday's lecture and some experimentation with the WordCount code I now have a solid understanding of how they work together, and how the application runs as a whole.

2. When the number of reducers is set to three, the result is that three reducers process the input and three output files are found in the output directory.

    ie. -D mapreduce.job.reduces=3
    part-r-00000 (32734 bytes)
    part-r-00001 (32849 bytes)
    part-r-00002 (31510 bytes)

    ie -D mapreduce.job.reduces=1
    part-r-00000 (97093 bytes)

It may be necessary to increase the number of reducers in the case of large output sets as we don't want all the data to be processed by one reducer, therefore slowing the time the application takes to execute. In the case of large output sets, we would want to have more reducers to utilize the power of the cluster effectively and decrease the execution time of the application. 

3. Setting the number of reducers to zero was different as the mappers directly output each key pair to the output file and completely bypassed the reducers. As the key pairs did not go through the reducers, there was no opportunity to go through the shuffle phase and sort the pairs in alphabetical order. Since this step was also skipped, identical keys were not grouped together, and there was no opportunity to add up occurrences of pairs with identical key values. 

The end result of using no reducers was (Text, Int) pairs in the order that they went through the mapper, with each key being equal to a word, and each integer value being 1.

    ie. Output text is like the following
    sussex  1
    their   1
    estate  1
    was     1
    large   1
    and     1

4. Yes. Once the combiner was optimized to reduce the amount of data sent to the shuffle, the application ran 20% faster (21sec -> 17sec) and used 10% less "Aggregate Resource Allocation". If we were running this application via AWS/GCP this would mean lower overall cost and faster execution time in practice. I think this is definitely a significant difference and outlines the importance of optimizing the combiner Class when using MapReduce.