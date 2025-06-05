import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read raw data from existing Glue catalog table
raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="cust-churn-s3-db", 
    table_name="cust_churn_bucket",
    transformation_ctx="raw_dyf"
)

# Convert to Spark DataFrame for column operations
df = raw_dyf.toDF()

# Drop 'Zip Code' column if exists
if 'Zip Code' in df.columns:
    df = df.drop('Zip Code')

# Drop duplicates
df = df.dropDuplicates()

# Convert back to DynamicFrame
cleaned_dyf = DynamicFrame.fromDF(df, glueContext, "cleaned_dyf")

# Write cleaned data back to S3 in CSV format
glueContext.write_dynamic_frame.from_options(
    frame=cleaned_dyf,
    connection_type="s3",
    connection_options={"path": "s3://query-s3result-bucket/cleaned_data/"},
    format="csv",
    transformation_ctx="write_to_s3"
)

job.commit()
