### Notes

### Questions

1. Take a screen shot of your list of EMR clusters (if more than one page, only the page with the most recent), showing that all have Terminated status.

2. 
    - What fraction of the input file was prefiltered by S3 before it was sent to Spark?

    - Comparing the different input numbers for the regular version versus the prefiltered one, what operations were performed by S3 and which ones performed in Spark?

3. 
    - Reviewing the job times in the Spark history, which operations took the most time? Is the application IO-bound or compute-bound?

    - Look up the hourly costs of the m6gd.xlarge instance on the EC2 On-Demand Pricing page. Estimate the cost of processing a dataset ten times as large as reddit-5 using just those 4 instances. If you wanted instead to process this larger dataset making full use of 16 instances, how would it have to be organized?

### Example

Replace bucket name w/ 
- c732-301305997-a5

--conf spark.yarn.maxAppAttempts=1 s3://BUCKET/relative_score_bcast.py s3://BUCKET/reddit-1 s3://BUCKET/output/reddit-1-1
