# Graded Assignment 3 – Week 4
## Click Count Analysis Using Google Cloud Dataproc and PySpark

### 1. Project Overview

This assignment demonstrates how to process clickstream data stored in Google Cloud Storage (GCS) using Apache Spark and PySpark on Google Cloud Dataproc.

The objective is to calculate the total number of clicks occurring in four different time-of-day intervals.

The solution is implemented using two approaches:

1. **RDD (Resilient Distributed Dataset) API**
2. **DataFrame API**

Both implementations are executed on Google Cloud Dataproc, with Google Cloud Storage used for input and output storage.

**Important:** SparkSQL is not used in this assignment.

---

### 2. Problem Statement

Given a text/CSV file containing `user_id` and `timestamp` columns, calculate the total number of clicks in the following intervals:

| Time Interval | Description |
|---|---|
| 0–6 hours | Midnight to before 6 AM |
| 6–12 hours | 6 AM to before 12 PM |
| 12–18 hours | 12 PM to before 6 PM |
| 18–24 hours | 6 PM to midnight |

Each record represents one click.

---

### 3. Technology Stack

- **Google Cloud Platform (GCP)**
- Google Cloud Storage (GCS)
- Google Cloud Dataproc
- Apache Spark
- PySpark
- Python

---

### 4. GCP Architecture

The assignment follows this processing architecture:

```text
          Input Dataset
          click_file.txt
                 |
                 v
       Google Cloud Storage
                 |
                 v
       Google Cloud Dataproc
          (Spark Cluster)
                 |
          +------+------+
          |             |
          v             v
     RDD API       DataFrame API
          |             |
    map()             withColumn()
          |             |
    reduceByKey()      groupBy()
          |             |
          v             v
     RDD Results    DataFrame Results
          |             |
          +------+------+
                 |
                 v
       Google Cloud Storage
                 |
          +------+------+
          |             |
          v             v
       RDD Output   DataFrame Output
```

---

### 5. GCP Resource Configuration

The following resources were used:

| Resource | Configuration |
|---|---|
| GCS Bucket | `week_4_assignment` |
| Bucket Location | US Multi-region |
| Input File | `click_file.txt` |
| Dataproc Cluster | `week4-pyspark-cluster` |
| Region | `us-east4` |
| Cluster Type | Development (Single Node) |
| Machine Type | `n4-standard-2` |
| Dataproc Version | `2.3.37-debian12` |
| Worker Nodes | 0 additional workers |
| Autoscaling | Disabled |

The Compute Engine default service account was assigned the Dataproc Worker role and the necessary Cloud Storage permissions.

---

### 6. Input Dataset

**File:** `click_file.txt`

**GCS Location:**

```text
gs://week_4_assignment/click_file.txt
```

**Dataset Structure:**

```csv
user_id,timestamp
u001,2025-08-01 09:03:23
u012,2025-08-01 12:37:50
u006,2025-08-01 06:57:32
u006,2025-08-01 21:09:33
```

Dataset details:

- Total records: 200 clicks
- Columns: `user_id`, `timestamp`
- Timestamp format: `YYYY-MM-DD HH:MM:SS`
- Date: August 1, 2025

---

### 7. PySpark Implementation

Both approaches are implemented in a single Python file:

`click_analysis.py`

#### A. RDD Implementation

The RDD implementation uses the following transformations and actions:

1. **textFile():** Reads the input dataset directly from GCS.
2. **filter():** Removes the CSV header.
3. **map():** Extracts the hour from each timestamp and assigns the corresponding time interval.
4. **reduceByKey():** Aggregates the number of clicks for each time interval.
5. **sortByKey():** Sorts the results by interval.
6. **saveAsTextFile():** Saves the output to GCS.

Output location:

```text
gs://week_4_assignment/output/rdd/
```

Spark generates partition-based output files, such as `part-00000` and `part-00001`, along with a `_SUCCESS` marker.

#### B. DataFrame Implementation

The DataFrame implementation uses built-in PySpark DataFrame operations:

1. **read.csv():** Reads the CSV dataset from GCS with the header enabled.
2. **to_timestamp():** Converts the timestamp column into a timestamp data type.
3. **hour():** Extracts the hour from the timestamp.
4. **withColumn():** Creates a new column containing the corresponding time interval.
5. **groupBy().count():** Groups records by time interval and calculates click counts.
6. **orderBy():** Sorts the results chronologically.
7. **write.csv():** Saves the output to GCS.

Output location:

```text
gs://week_4_assignment/output/dataframe/
```

The DataFrame output is stored as CSV files with a header.

---

### 8. Execution on Google Cloud Dataproc

The PySpark program was executed using the Dataproc Jobs interface.

Execution procedure:

1. Uploaded `click_analysis.py` to the GCS bucket.
2. Opened the Dataproc cluster.
3. Selected the **Submit Job** option.
4. Selected `PySpark` as the job type.
5. Specified the main Python file:

```text
gs://week_4_assignment/scripts/click_analysis.py
```

6. Submitted the job.
7. Monitored execution through the Dataproc Jobs interface.
8. Verified successful execution through job output and logs.
9. Verified both output directories in GCS.

The job completed successfully and displayed:

```text
RDD Results:
...

DataFrame Results:
...

Week 4 assignment completed successfully!
```

---

### 9. Results and Output Verification

Both implementations produced identical results.

| Time Interval | RDD Click Count | DataFrame Click Count |
|---|---:|---:|
| 0–6 hours | 44 | 44 |
| 6–12 hours | 53 | 53 |
| 12–18 hours | 48 | 48 |
| 18–24 hours | 55 | 55 |
| **Total** | **200** | **200** |

**Result:** Both implementations successfully calculated the click counts for all four time intervals.

The output files were downloaded from GCS and verified.

---

### 10. Project Structure

```text
WEEK_4/
│
├── click_analysis.py
│
├── click_file.txt
│
├── output/
│   │
│   ├── rdd/
│   │   ├── part-00000
│   │   ├── part-00001
│   │   └── _SUCCESS
│   │
│   └── dataframe/
│       ├── part-00000-*.csv
│       └── _SUCCESS
│
└── README.md
```

*Note: Spark may generate additional metadata and partition files. The structure above represents the main assignment artifacts.*

---

### 11. Key Learnings

Through this assignment, I gained practical experience with:

- Understanding Apache Spark and PySpark.
- Working with RDDs and DataFrames.
- Applying distributed data processing transformations.
- Aggregating data using `reduceByKey()` and `groupBy().count()`.
- Processing timestamp-based data.
- Reading input files directly from Google Cloud Storage.
- Writing distributed Spark outputs to GCS.
- Creating and configuring a Dataproc cluster.
- Submitting and monitoring PySpark jobs on Google Cloud.

---

### 12. Conclusion

This assignment successfully demonstrates distributed data processing using PySpark on Google Cloud Dataproc.

The same click-counting problem was implemented using both RDDs and DataFrames. Both approaches generated identical results, confirming the correctness of the implementations.

Google Cloud Storage was used for input and output storage, while Dataproc provided the managed Apache Spark environment for executing the jobs.

**Final Status: Successfully Completed.**