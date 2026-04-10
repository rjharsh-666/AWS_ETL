import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1775833693257 = glueContext.create_dynamic_frame.from_catalog(database="sample_db", table_name="customers", transformation_ctx="AmazonS3_node1775833693257")

# Script generated for node Drop Fields
DropFields_node1775833708241 = DropFields.apply(frame=AmazonS3_node1775833693257, paths=["phone_2"], transformation_ctx="DropFields_node1775833708241")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1775833708241, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775833498504", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1775833713722 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1775833708241, connection_type="s3", format="glueparquet", connection_options={"path": "s3://harsh-s3-1-bucket/transformed/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1775833713722")

job.commit()