## Answers

1. Do people from Ontario tend to put larger purchases on one payment method?

    a. Prose answer to the question

    For people in Ontario, it seems that people put large purchases on their credit cards rather than their debit cards. The average purchase price that people made with a credit card was 131.40$ and the average purchase price that people made with their debit cards was 101.06$. I have provided the data returned from the query below.

    +--------------+--------------+
    |mtype         |avg           |
    +--------------+--------------+
    |Credit        |131.40        |
    |Debit         |101.06        |
    +--------------+--------------+

    b. Your query used to arrive at this answer

      SELECT final.mtype, AVG(final.amount) 
      FROM 
      (SELECT pm.amount, pm.mtype, customers.province 
      FROM customers JOIN 
      (SELECT purchases.pmid, purchases.custid, purchases.amount, paymentmethods.mtype 
      FROM purchases 
      JOIN paymentmethods 
      ON (purchases.pmid = paymentmethods.pmid)) AS pm 
      ON (pm.custid=customers.custid)) AS final 
      WHERE final.province='ON' 
      GROUP BY final.mtype;

2. Consider the three groups of people: people who live in the Vancouver region, visitors from other BC areas, and visitors from outside BC altogether. Which group spent the most per transaction?

    a. An answer to the original question in prose

    When considering people in Vancouver, people in other areas of BC, and people from outside of BC, we can say that people from outside of the province tend to spend more per transaction. The median amount that people spend in the Vancouver region was $27.37, the median amount for other BC residents was $30.08, and the median amount for people outside of BC was $33.27. Note that the sample size for people from BC who live outside of the greater Vancouver area is significantly smaller than the other two groups. I have included the data returned from the query below.
    
    Also, it is important to note that in practice we typically would not make concrete claims based on one statistic (ie. median), and should analyze the data further in order to find more evidence to either support our findings or refute them. We can see that the average values are in a consistent order as the median values across the three groups, and suggest that we could be right in claiming that people outside of BC tend to spend more per transaction.

    +--------------+-----------------+---------+----------+----------+
    |from_van      |from_bc_non_van  |count    |       avg|    median|
    +--------------+-----------------+---------+----------+----------+
    |1             |0                |10384    |     86.01|    27.370|
    |0             |1                |3899     |     95.16|    30.080|
    |0             |0                |15717    |    112.89|    33.270|
    +--------------+-----------------+---------+----------+----------+

    b. A SQL statement to create the required view

      CREATE VIEW vancouver_custs AS
      SELECT
      ct.custid,
      CASE WHEN gvp.pcprefix IS NULL THEN 0 ELSE 1 END AS in_vancouver
      FROM (SELECT 
      customers.custid,
      customers.postalCode,
      SUBSTRING(customers.postalCode, 1, 3) AS pc 
      FROM customers) AS ct
      LEFT JOIN
      (SELECT DISTINCT pcprefix as pcprefix FROM greater_vancouver_prefixes) AS gvp
      ON gvp.pcprefix=ct.pc;

    c. A SQL query to support your answer for component a

      SELECT 
      final.in_vancouver, 
      final.from_bc_non_van,
      COUNT(final.amount) AS count,
      AVG(final.amount) AS avg,
      MEDIAN(final.amount) AS median
      FROM
      (SELECT
      vantable.in_vancouver,
      vantable.from_bc_non_van,
      purchases.amount
      FROM 
      purchases
      JOIN
      (SELECT
      customers.custid,
      vancouver_custs.in_vancouver,
      CASE WHEN customers.province='BC' AND vancouver_custs.in_vancouver=0 THEN 1 ELSE 0 END AS from_bc_non_van
      FROM customers
      JOIN vancouver_custs
      ON customers.custid=vancouver_custs.custid) AS vantable
      ON purchases.custid=vantable.custid)
      AS final
      GROUP BY final.in_vancouver, final.from_bc_non_van
      ORDER BY median;

3. Who spends more at restaurants that serve sushi: locals (residents of Greater Vancouver) or tourists?

    a. An answer to the original question in prose

      Based on the average price that residents of Vancouver pay for sushi vs tourists, we can say that tourists tend to pay slightly more. We can see below that the average price that a Vancouver resident will pay is $77.57 whereas the average price that someone from outside of the greater Vancouver area will pay for sushi is $85.80. Note that the tourist group contains anyone who made a purchase in the sushi restaurants who was not a resident of the greater Vancouver area (ie. from other areas of bc or from other provinces).

      +--------------+------------+
      |from_van      |avg         |
      +--------------+------------+
      |1             |77.57       |
      |0             |85.80       |
      +--------------+------------+

    b. A SQL query to support your answer for component a

      SELECT AVG(final.amount) AS avg, final.in_vancouver AS in_vancouver 
      FROM
      (SELECT
      CASE WHEN vancouver_custs.in_vancouver=1 THEN 1 ELSE 0 END AS in_vancouver,
      sushipurchase.amount
      FROM
      (SELECT
      purchases.custid,
      purchases.amount
      FROM purchases
      JOIN
      (SELECT * FROM amenities
      WHERE amenities.tags.cuisine ILIKE '%sushi%' AND 
      amenities.amenity='restaurant') AS sushiplaces
      ON purchases.amenid=sushiplaces.amenid)
      AS sushipurchase
      LEFT JOIN vancouver_custs
      ON sushipurchase.custid=vancouver_custs.custid)
      AS final
      GROUP BY final.in_vancouver;

4. What was the average purchase per day for the first five days of August?

    a. An answer to the original question in prose

      The average purchase price per day for the first five days was as follows. August 5th had the lowest average purchase price with $95.67 and August 4th had the highest average purchase price with $115.50. The data returned from the query is shown below.

      +--------------+--------------+
      |pdate         |avg           |
      +--------------+--------------+
      |2021-08-01    |96.59         |
      |2021-08-02    |106.56        |
      |2021-08-03    |95.87         |
      |2021-08-04    |115.50        |
      |2021-08-05    |95.67         |
      +--------------+--------------+

    b. A SQL query for Redshift to support your answer for component a

      SELECT pdate, AVG(amount) AS avg
      FROM purchases
      WHERE date_part(mon, pdate)=8 AND date_part(d, pdate)<=5
      GROUP BY pdate
      ORDER BY pdate;

    c. What was the bytes/record ratio for Redshift on the 5-day query?

      When using Redshift for the 5-day query, the "Seq Scan on purchases" row had 94.06KB (94060 Bytes) and 4703 rows. We can say that the bytes to record ratio, in this case, was (94060/4703) or 20 bytes per record.

    d. What was the bytes/record ratio for Spectrum on the 5-day query?

      The bytes record ratio for spectrum on the 5-day query was 267396 bytes / 4703 rows or roughly 56.8 bytes per record. The table that was returned when querying Redshift's internal management table to get the number of bytes read is shown below.

      The first row corresponds to the query done on 31 days, and the second row was the query done on 5 days.

      +---------------------------------+------------------+----------------+------------------------+------------------------+
      |external_table_name              |s3_scanned_bytes  |s3_scanned_rows | avg_request_parallelism| avg_request_parallelism|
      +---------------------------------+------------------+----------------+------------------------+------------------------+
      |S3 Subquery dev_s3_ext_purchases |1705186           |30000           |                     4.3|                      10|
      |S3 Subquery dev_s3_ext_purchases |267396            |4703            |                     1.3|                       5|
      +---------------------------------+------------------+----------------+------------------------+------------------------+


    e. For this purchase dataset, the averages are 57 bytes/line and 968 lines/day. (It may be useful to explore the public-cmpt-732 bucket to derive these for yourself.) From these values, what might you infer about how Redshift scans the table? How Spectrum scans the table?
      
      Based on the ratios we calculated above we can see that redshift is reading less than half as many bytes as Spectrum (Redshift read 20 bytes/line whereas Spectrum read ~57 bytes/line). 
      
      Since Spectrum is reading directly from S3, Spectrum has to read the entirety of every row and select the relevant information on the fly. Due to this typical relational database style, more bytes are being read per line. This is where the benefits of redshifts "columnar storage organization" comes in handy. Redshift is only reading the columns that are necessary to execute the query. This columnar storage organization in redshift significantly reduces the amount of data scanned when making the query.

    f. Based on these computations and the parallelism results, what properties of a dataset might make it well-suited to loading from S3 into Redshift before querying it?

      Let's say that we have a dataset that contains a large number of columns that contain information that will not be needed for every query. For example, we could have a database of countries and hundreds of columns of features (population, gdp, land_area, avg_age, num_females, num_males etc.). If we are going to be making a very large number of queries on this table that only require a few columns at a time, it would be wise to utilize redshift to make these queries (given we have enough nodes to maximize parallelization). Not having to scan the entire row from s3 on every query will make each query faster and more efficient.

      In terms of parallelization, since we are creating a cluster in redshift we have a predefined number of nodes to which the rows are distributed. On the other hand, Spectrum allocates nodes based on need and therefore does not have a fixed number of nodes that can be used. This means that Spectrum is more suited to scalable parallelization than the redshift cluster that we are using (which has one node). It is important to note that storing data in redshift is much more expensive, and it is also worth looking at the frequency at which we are expecting to query the data when choosing between using Spectrum or Redshift. This is because Spectrum charges on a per-byte scanned rate.

    g. Conversely, what properties of a dataset might make it well-suited to retaining in S3 and querying it using Spectrum?

      Let's say that an organization has a large dataset and only wants to make infrequent queries using a dataset with very few columns that are all used on every query, then Spectrum could potentially be the better choice. If every query requires reading every column in the dataset then the benefits of using purely redshift are minimized. Since Spectrum is serverless and the data is stored in S3, we only pay based on the amount of data scanned and the minimal s3 storage charges (as compared to redshift storage cost). If the organization plans to make infrequent queries using all the columns of the dataset, then the case for using Spectrum becomes more likely. The frequency of queries and the nature of these queries are all things that need to be taken into account when determining whether to use redshift or spectrum when querying our data.