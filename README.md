# Project-Databricks-Skew-AQE-Solutions

Demonstrates AQE to optimize skewed joins using the Online Retail Dataset in Databricks

## Objectives

- Configure AQE with skew join optimization to mitigate data skew.
- Load and process the Online Retail Dataset with a manually created small lookup table to simulate a join scenario.
- Analyze the Spark UI to evaluate AQE's impact on query performance.
- Document findings and prepare the project for portfolio presentation.

## Methodology

1. **Environment Setup**: Utilized Databricks with Spark, enabling AQE configurations (`spark.sql.adaptive.enabled=true`, `spark.sql.adaptive.skewJoin.enabled=true`, `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB`).
2. **Data Preparation**: Loaded the Online Retail Dataset (`/FileStore/tables/online_retail.csv`) and created a small `customer_df` (3 rows) to simulate a skewed join key.
3. **Join Execution**: Performed a left outer join on `CustomerID` with `show(5)` to trigger query execution, limiting the dataset to 48 rows for analysis.
4. **Spark UI Analysis**: Reviewed the SQL/DataFrame and Stages tabs to assess AQE's optimization (e.g., join strategy, task durations, input sizes).
5. **Documentation**: Recorded observations in a Databricks notebook, exported as `.ipynb` for GitHub.

## Findings

- **AQE Optimization**: AQE dynamically switched from a planned SortMergeJoin to a BroadcastHashJoin, leveraging the small `customer_df` (96 bytes, 3 rows) to reduce shuffle overhead.
- **Performance**: The query completed in 4 seconds, with Stage 9 (join) taking 0.3 seconds and processing 7.0 KiB (48 rows) from `retail_df`.
- **Skew Handling**: No `SkewJoin` was triggered, as the dataset size (below 256 MB threshold) didn’t exhibit significant skew, aligning with the educational scope using a subset.
- **Learning Insight**: AQE’s adaptability was evident, but skew optimization requires a larger dataset (e.g., full 541,909 rows) to observe partition splitting.

## Technical Details

- **Dataset**: Online Retail Dataset (subset of 48 rows for `show(5)`).
- **Spark Version**: Databricks runtime (specific version per cluster configuration).
- **AQE Configurations**:
  - `spark.sql.adaptive.enabled`: true
  - `spark.sql.adaptive.skewJoin.enabled`: true
  - `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`: 256 MB
- **GitHub Commit**: Pushed on June 11, 2025, at 10:35 PM IST.

## Conclusion

This project successfully demonstrated AQE’s ability to optimize joins by adapting to runtime statistics, even with a small dataset. The absence of skew handling highlighted the need for larger data to fully explore AQE’s skew mitigation capabilities. The notebook and this README serve as a portfolio piece, showcasing data engineering skills in Spark optimization and Git version control.
