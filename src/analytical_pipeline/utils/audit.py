# src/utils/audit.py

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
   StructType,
   StructField,
   StringType,
   TimestampType,
   LongType,
)
class AuditLogger:

    def __init__(
        self,
        table_name: str
    ):
        self.spark = SparkSession.getActiveSession()
        self.table_name = table_name

    def start_run(
        self,
        run_id: str,
        job_name: str,
        task_name: str
    ) -> None:

        audit_data = [
            (
                run_id,
                job_name,
                task_name,
                datetime.now(),
                None,
                "PROCESSING",
                None,
                None
            )
        ]

        schema = StructType([
           StructField("run_id", StringType(), True),
           StructField("job_name", StringType(), True),
           StructField("task_name", StringType(), True),
           StructField("start_time", TimestampType(), True),
           StructField("end_time", TimestampType(), True),
           StructField("status", StringType(), True),
           StructField("records_processed", LongType(), True),
           StructField("error_message", StringType(), True),
       ])

        (
            self.spark.createDataFrame(audit_data, schema)
            .write
            .mode("append")
            .saveAsTable(self.table_name)
        )

    def mark_success(
        self,
        run_id: str,
        task_name: str,
        records_processed: int = None
    ) -> None:

        records_value = (
            str(records_processed)
            if records_processed is not None
            else "NULL"
        )

        self.spark.sql(f"""
            UPDATE {self.table_name}
            SET
                status = 'SUCCESS',
                end_time = current_timestamp(),
                records_processed = {records_value}
            WHERE run_id = '{run_id}'
              AND task_name = '{task_name}'
              AND status = 'PROCESSING'
        """)

    def mark_failed(
        self,
        run_id: str,
        task_name: str,
        error_message: str
    ) -> None:

        escaped_error = error_message.replace("'", "''")

        self.spark.sql(f"""
            UPDATE {self.table_name}
            SET
                status = 'FAILED',
                end_time = current_timestamp(),
                error_message = '{escaped_error}'
            WHERE run_id = '{run_id}'
              AND task_name = '{task_name}'
              AND status = 'PROCESSING'
        """)
