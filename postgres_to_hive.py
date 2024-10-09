import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.io.parquetio import WriteToParquet
import pyarrow as pa
import logging
from datetime import datetime


class FilterNewRecords(beam.DoFn):
    def process(self, element, last_processed_timestamp):
        # Convert the created_at string to a datetime object
        created_at = datetime.strptime(element[1], '%Y-%m-%d %H:%M:%S.%f')
        last_processed_timestamp = datetime.strptime(last_processed_timestamp, '%Y-%m-%d %H:%M:%S')

        # Filter new records based on timestamp
        if created_at > last_processed_timestamp:
            yield {
                'id': element[0],
                'created_at': created_at
            }


def run():
    logging.basicConfig(level=logging.DEBUG)
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    p = beam.Pipeline(options=pipeline_options)

    # Define JDBC options for PostgreSQL
    jdbc_url = "jdbc:postgresql://host.docker.internal:5435/postgres"
    jdbc_props = {
        'driver': 'org.postgresql.Driver',
        'user': 'postgres_user',
        'password': 'postgres_password'
    }
    #Define the postgres user and Password as it is called in line 45 &46

    # Read data from PostgreSQL
    rows = (
        p
        | "ReadFromPostgres" >> ReadFromJdbc(
            table_name="time_table",
            jdbc_url=jdbc_url,
            username=jdbc_props['user'],
            password=jdbc_props['password'],
            driver_class_name=jdbc_props['driver'],
            query="SELECT id, to_char(created_at, 'YYYY-MM-DD HH24:MI:SS.US') as created_at FROM time_table"
        )
        # Convert the tuple to a dictionary and filter rows based on timestamp
        | "FilterNewRows" >> beam.ParDo(FilterNewRecords(), last_processed_timestamp='2023-01-01 00:00:00')
    )

    # Define the schema using PyArrow
    schema = pa.schema([
        ('id', pa.int64()),
        ('created_at', pa.timestamp('s'))
    ])

    # Write data to Parquet format for Hive
    rows | "WriteToParquet" >> WriteToParquet(
        file_path_prefix='/usr/local/hadoop/parquet_output',   #Adjust prefered path for parquet files by beam
        schema=schema
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
