# Data Engineering: Real-Time Apache Beam Pipeline from PostgreSQL to Hive

This project implements a real-time data pipeline using **Apache Beam** to update a **Hive** database based on changes in a **PostgreSQL** database. The pipeline reads new records from PostgreSQL, processes them, and writes the results to Hive in **Parquet** format.
All of this is done on local Macos/Linux Terminal

## Prerequisites

Ensure you have the following installed:

- **PostgreSQL** (version 14 or higher)
- **Hive** (version 3.1.3 or higher)
- **Python** (version 3.11 or higher)
- **Java 8** (for Apache Beam)
- **Apache Hadoop** (for Hive)
- **Apache Beam** (installed via Python)

## Pipeline Overview

1. **Source**: PostgreSQL database table (eg `time_table`) which captures real-time events.
2. **Transformation**: The pipeline processes and filters new records based on a timestamp.
3. **Sink**: Writes the filtered data into Hive, stored in **Parquet** format.

## Setup Steps

1. **Clone the Repository**
   ```bash
   git clone https://github.com/MboeJua/Data-Eng---Apache-Beam-Postgresl---Hive.git
   cd Data-Eng---Apache-Beam-Postgresl---Hive
   ```

2. **Install Dependencies**
   Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure PostgreSQL**
   - Setup Ensure PostgreSQL is running and accessible.
   - Set up a user with proper privileges (`hive` user).
   - Update the connection parameters in the Beam pipeline script (e.g `beam_postgres_to_hive.py`).
   - creation of a table for data changes based on step 4

4. **Creation of a cron job for insert in Postgresql**
   - In terminal create a new cron job with insert job to created table in step 3
   ```nano ~/postgres_cron_job
   * * * * * /usr/local/bin/psql -U your_user -d your_db -c "INSERT INTO your_table (data, timestamp) VALUES ('new_data', NOW());"
   ```
   # /usr/local/bin/psql : path to your psql in directories
   - Add the new cron job to other jobs running and verify
   ```crontab ~/my_cron_job
   crontab -l
   ```

6. **Configure Hive Metastore**
   - Use PostgreSQL as the metastore backend for Hive.
   - Initialize the metastore schema in PostgreSQL using `schematool`.

7. **Run the Apache Beam Pipeline**
   To run the pipeline that reads from PostgreSQL and writes to Hive:
   ```bash
   python beam_postgres_to_hive.py
   ```

8. **Verifying Data in Hive**
   Once the pipeline is executed, check the output in Hive:
   ```bash
   start-dfs.sh
   hive --service metastore &
   hive
   hive> SHOW TABLES;
   hive> SELECT * FROM time_table_parquet;
   ```

## Hive & HDFS Configuration

Ensure that Hive is correctly configured with HDFS for storing the output Parquet files. Verify that HDFS services are running and the necessary directories are created in `/user/hive/warehouse`.

## Common Issues

- **Permission Denied**: Ensure PostgreSQL and Hive have the necessary permissions.
- **Metastore Errors**: Ensure Hive metastore is correctly initialized and running.
- **Java/Beam Issues**: Ensure correct versions of Java and dependencies are installed. (Java 8 used for Hive as later version have dependencies issues)
