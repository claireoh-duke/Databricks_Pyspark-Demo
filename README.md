# Databricks_Pyspark-Demo

### COVID-19 Spark Pipeline
Dataset Description
Source: Databricks COVID-19 public dataset​
Format: CSV files with daily COVID-19 counts by state and county
Columns include: date, county, state, cases, deaths, and fips
Rows: 1,227,256

### Pipeline & Analysis Summary
Spark pipeline covers: data cleaning, filtering, group-by aggregations, derived columns, and efficient data writing. Filters are applied early to minimize scan cost. Data types are explicitly cast and malformed values (non-integer deaths) are handled gracefully. Final results are written in Delta format for optimized downstream use.

### Core Steps
1. Read & Inspect: Load CSV, display schema, show row count and preview data.
2. Cleaning & Type Casting: Use withColumn and when/rlike to cast date, cases, and deaths precisely. Non-integer deaths are set to NULL for downstream compatibility.
3. Early Filtering: Filter out rows with nulls in critical columns (cases, deaths). Only rows with complete data are processed—which speeds up subsequent steps and reduces shuffle cost.
4. Aggregation: State-level death summaries and averages via groupBy and agg (count, sum, avg).
5. Write-out: Save results to Delta table, enabling fast re-access and ACID guarantees.

### Performance Analysis
**How Spark Optimized**
Pushed projections and filters down to file scans, performed early predicate evaluation (e.g., state IS NOT NULL), and used Photon's vectorized, columnar operators (PhotonGroupingAgg, PhotonShuffledHashJoin) for faster processing.
**Where Filters Pushed Down**
Filters were pushed down to the FileScan stage, resulting in filtering before data was loaded or moved downstream, which minimized disk I/O.
**Performance Bottlenecks**
The primary bottleneck was the Shuffle stage triggered by GroupBy and Join operations, along with associated shuffle cost and data skew.
**Pipeline Optimization**
1. Filter Ordering: Implemented a "filter early" strategy to reduce data volume before expensive shuffles/aggregations
2. Column Pruning: Read only referenced columns during FileScan to cut I/O
3. Shuffle Control: Set spark.sql.shuffle.partitions = 10 and repartitioned by key (state) to reduce skew/cost
4. Output: Wrote to Parquet partitioned by year to speed up time-based reads

### Key Finding
- Earliest COVID-19 county entries appear in WA/IL/CA in late January 2020; New York counties rise sharply in early March.
- After correcting for cumulative time series (latest row per county), state-level totals are consistent with rollups from the underlying county data.
- Partitioning by year improves time-bounded scans, and repartitioning by state reduces shuffle contention for state-keyed joins/aggregations.

### Screeshot 
1. Physical plan:
  ![Physical plan_filtered](/Users/otting/Desktop/g.png)
  ![Physical plan_groupby](/Users/otting/Desktop/f.png)
2. Query details:
  ![Query Details](/Users/otting/Desktop/q.png)
3. Successful writes:
  ![Successful writes](/Users/otting/Desktop/s.png)
