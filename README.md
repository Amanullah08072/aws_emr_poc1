## 🚀 PoC Phase 2: Operational Serving Layer Extension

### 📋 Overview
This project represents the continuation of Phase 1 (Raw Ingestion -> EMR Spark Processing -> S3 Partitioned Gold Zone). Phase 2 focuses on bridging big data storage with near-real-time downstream transactional systems. 

We transition the computation layer to a serverless architecture using **AWS Glue PySpark ETL** to aggregate, filter, and stream clean data lake metrics over a JDBC channel directly into an **Amazon Aurora PostgreSQL Serverless v2** cluster. This operational warehouse layer serves as the optimized database backend designed to power live microservice APIs and operational dashboard applications.

### 📐 Architecture
The expanded data pipeline implements the following lifecycle topology:
1. **Data Lake Storage:** Multi-month partitioned Parquet collections hosted securely in `gold-zone-poc1`.
2. **Cataloging & Metadata:** Data schema resolution via the AWS Glue Data Catalog.
3. **Compute Engine:** Serverless AWS Glue Spark engine running a optimized PySpark target load script (`poc1_demo1`).
4. **Private Network Routing:** Private traffic control managed inside a customer VPC via an S3 Gateway Endpoint (`vpce-07f17a458b4343f02`).
5. **Relational Serving Layer:** Multi-tenant relational storage using Amazon Aurora Serverless v2 (PostgreSQL).

---

### 🧠 Core Engineering Challenges Overcome

#### 1. Mitigation of VPC Network Isolation (S3 Routing Failure)
* **The Problem:** Attaching a private JDBC connection forces AWS Glue Elastic Network Interfaces (ENIs) directly into private subnets. This strips the Spark executors of their default internet routing, leading to immediate communication failures when attempting to locate public S3 asset buckets.
* **The Solution:** Provisioned an internal **S3 Gateway VPC Endpoint** within the network infrastructure. This explicitly appended dedicated routing entries to the subnet's Route Tables, establishing a secure, highly efficient, private AWS internal backbone bridge to stream data safely.

#### 2. IAM Resource Constraint Resolution
* **The Problem:** Granular security boundaries initially triggered `403 Forbidden` evaluation bugs due to improper ARN string formatting (`arn:aws:s3:::s3://...`) and lack of namespace exploration context.
* **The Solution:** Standardized resource targeting protocols to separate object manipulation keys (`arn:aws:s3:::gold-zone-poc1/*`) from directory-level listing authorizations (`s3:ListBucket` on the root ARN bucket string).

---

### 📊 Verification & Execution
To verify consistency, database states were cleared and the serverless ETL deployment was evaluated:
* **Glue Execution Window:** 1 minute 31 seconds.
* **Target Load Integrity:** Confirmed successful operational payload delivery via CloudShell CLI using PostgreSQL query checks:

```sql
SELECT * FROM ecommerce_performance_summary;
-- Outputs fully aggregated rows (Beauty, Books, Automotive, etc.) mapping Gross Revenue, Conversion Rates, and Clicks.
