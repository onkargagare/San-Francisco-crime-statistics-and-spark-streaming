1.How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

--> The processedRowsPerSecond increase and decrease affected the throughput and latency of the data, the next batch
of data is kept on hold until the current batch is processed.

2.What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

--> processedRowsPerSecond depends on these settings-
spark.streaming.kafka.maxRatePerPartition
spark.default.parallelism
spark.sql.shuffle.partitions
