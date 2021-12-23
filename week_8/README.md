## Questions

How much should we repartition the dataframe by? Is it more efficient to partition the rdd before creating the dataframe?

Some select statements worked and some did not! Interesting. Why is that?
- Some rows are stored on some nodes and others on other nodes, why are we able to see the whole table when a node is down with "SELECT * FROM test"?

## Answers

1. What happened when you inserted another row with the same primary key as an existing row?

When I inserted another row into the dataframe which had a primary key that already existed, that row was "overwritten" with the new data that was passed in. In the example, we are trying to create a new row with the existing primary key "2" with the following line.

    INSERT INTO test (id, data) VALUES (2, 'double');

In the table, there already exists the following row with the data (2, 'secondary') which is replaced with the new values that we are trying to insert (2, 'double'). If we want to add new rows into the Cassandra table we need to make sure we are using unique keys otherwise we will be overwriting existing rows in the database. Cassandra does support secondary keys so each primary key does not necessarily have to be unique, but the overall combination of the keys must be unique in order to create a new row in the table.

2. What happened when you query a keyspace with replication factor 1 and one node down? How did it behave with replication factor 2 and a node down?

After altering the replication factor to 1, I experimented with making queries on the simple test table we created. Once the unreliable3.local node went down, I was unable to make queries. When I tried to query the cassandra table and select all elements with the following "SELECT * FROM test", it threw a NoHostAvailable error.

Reading deeper into the error message it said that the required_replicas=1 and alive_replicas=0. Having no alive_replicas meant that we could not query the data until the "reliable-unreliable node 3" came back online. If the replication factor was higher, the data would be replicated across multiple nodes and we could still query the data when unreliable3.local was down. I have included the error message when querying the test table with the replication factor set to 1 below.

    NoHostAvailable: ('Unable to complete the operation against any hosts', {<Host: 10.17.202.217:9042 datacenter1>: Unavailable('Error from server: code=1000 [Unavailable exception] message="Cannot achieve consistency level ONE" info={\'consistency\': \'ONE\', \'required_replicas\': 1, \'alive_replicas\': 0}')})

After another up/down cycle I set the replication factor back to 2 and tested a few queries. Now that the data was replicated on multiple nodes, when one node went down I was still able to access and query the data as it was available on a live node. 

    CREATE KEYSPACE baa30 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2 };


3. How did the consistency level affect the results with the node up/down?

Changing the consistency levels was quite interesting. With consistency levels set to "ONE" we are saying that a write must be written to at least one replica node on the cluster. On the other hand, when we set consistency levels to "ALL" we are saying that the write must be written to all replica nodes on the cluster. When the Node was down and the consistency levels are set to "ONE" we are able to make queries and insert new rows into the table yet when we set the consistency levels to "ALL" some queries worked and others did not. 

For example, we were unable to select all elements from the test table, insert new rows with certain primary keys, and sele dct specific primary keys with consistency set to all. This is because we are requiring consistency across all nodes yet one of our nodes is down. I have included a list of statements that worked when consistency was set to all and those that did not. 

Worked: 
    SELECT * FROM test WHERE id=1;
    INSERT INTO test (id, data) VALUES (7, 'sevenish');
    SELECT * FROM test WHERE id=4;
    SELECT * FROM test WHERE id=5;

Did not Work:
    INSERT INTO test (id, data) VALUES (10, 'zehn');
    SELECT * FROM test;
    SELECT * FROM test WHERE id=2;
    SELECT * FROM test WHERE id=3;

Once all the nodes were up and running, all of the queries above ran seemlessly with consistency set to ALL as the number of live replicas was equal to the number of required replicas.

4. Which of the WHERE id=? values returned successfully with CONSISTENCY ALL when one of the nodes was down? Why do you think some could be returned but not others?

This is all dependant on each row's primary key. The primary key is basically a partition key that determines which node stores the data and where to find that data when needed. This primary key is a unique indentifier, and each primary key must be unique.

Futhermore, when a node goes down, we are only able to access the records that are being stored on the nodes that are up and running. This is why we ran into errors when one of the nodes went down because some of the records that we are trying to access are being stored on the unreliable node. When the unreliable node went down we were only able to access records with id's 1,4,5 yet we were still unable to access the record with id's 2 and 3.

Worked: 
    SELECT * FROM test WHERE id=1;
    SELECT * FROM test WHERE id=4;
    SELECT * FROM test WHERE id=5;

Did not Work:
    SELECT * FROM test WHERE id=2;
    SELECT * FROM test WHERE id=3;

In the case that we were using compound primary keys, this would be easier to visualize. For compound primary keys, the primary key is determined by multiple values. For example, a primary key could look like the following (PRIMARY KEY (name, place)). In this example, each record with the same "name" will be stored on the same node. So, if we found a record that was inaccessible when the unreliable node was down, then all other records with that same name would be inaccessible as well.

5. What was the CREATE TABLE statement you used for the nasalogs table? What was the primary key you choose, and why?

I chose to implement a compound primary key with the host as the first component and a UUID field as the second component.

Using a pyspark shell I found that the maximum number of times we saw the same host was 66 times in nasa-logs-1 out of 101 unique host names. I did this to make sure that we would have the data distributed evenly across all the nodes. Given a case where there are only 2 or 3 hosts and one host is responsible for 90% of the records, then we would have an uneven distrbution of data across the nodes and should look at using a different component for the primary key. That being said in our case using the host as the first component looks promising.

Since we are going to be making queries based on the host name when we calculate pearsons R this also ensures the data we want to operate on is stored on the same node. Finally, in using the UUID field we are pretty much ensuring that each record in the table is unique. Note that the probability of creating an identical 128-bit UUID is not actually zero but it is so ridiculously low that we assume uniqueness.

Here is the code I used to manually create the nasalogs table.

    CREATE TABLE nasalogs (
    host TEXT,
    datetime TIMESTAMP,
    path TEXT,
    bytes INT,
    uniq_id UUID,
    PRIMARY KEY (host, uniq_id)
    );


6. What was the CQL query you used (or tried) to get the total number of bytes?

I tried to use the CQL query below to get the total number of bytes but as expected when nasa-logs-2 was loaded into the cassandra table the query got an "..Operation timed out.." error. The query and the error message are shown below.

    SELECT sum(bytes) FROM nasalogs;

    ReadTimeout: Error from server: code=1200 [Coordinator node timed out waiting for replica nodes' responses] message="Operation timed out - received only 0 responses." info={'consistency': 'ONE', 'required_responses': 1, 'received_responses': 0}