# Spark ETL Sample Project

The goal of this project is to perform an extract, transform and load (ETL) process to migrate data into a local Apache Spark cluster.

* Language: **Python**
* Technologies: **Spark, GPG Encryption**

# ETL Process
1. Decrypt local GPG-encrypted CSV files
2. Load CSV tabular data into Spark DataFrames
3. Save DF data to Parquet files
4. Write query to determine average age
5. Write query to determine age at the 75th percentile

# Approach
1. Ask questions to get clarification
2. Install Apache Spark (note: if you experience too much trouble with setting up spark locally, then you may use duckdb instead)
3. Write code in Python using data files and GPG keys stored in this repo
4. Commit code to your repo and share link
