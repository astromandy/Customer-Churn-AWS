from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *  # noqa: F401,F403 - standard AWS Glue import style
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


def get_optional_arg(name: str, default: str) -> str:
    flag = f"--{name}"
    prefix = f"{flag}="

    for arg in sys.argv:
        if arg.startswith(prefix):
            return arg.split("=", 1)[1]

    if flag in sys.argv:
        flag_index = sys.argv.index(flag)
        if flag_index + 1 < len(sys.argv):
            return sys.argv[flag_index + 1]

    return default


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
source_database = get_optional_arg("SOURCE_DATABASE", "churn_db")
source_table = get_optional_arg("SOURCE_TABLE", "cust_churn_raw")
output_path = get_optional_arg(
    "OUTPUT_PATH", "s3://cust-churn-processed-data/cleaned_data/"
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

raw_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=source_database,
    table_name=source_table,
    transformation_ctx="raw_dyf",
)

df = raw_dyf.toDF()

if "Zip Code" in df.columns:
    df = df.drop("Zip Code")

df = df.dropDuplicates()

cleaned_dyf = DynamicFrame.fromDF(df, glue_context, "cleaned_dyf")

glue_context.write_dynamic_frame.from_options(
    frame=cleaned_dyf,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet",
    transformation_ctx="write_to_s3",
)

job.commit()
