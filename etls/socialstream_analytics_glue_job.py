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
AmazonS3_node1736049064231 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
                                                                           connection_type="s3",
                                                                           format="csv",
                                                                           connection_options={"paths": ["s3://social-insights-data-atharv-01/raw/socialstream_20250105.csv"], "recurse": True},
                                                                           transformation_ctx="AmazonS3_node1736049064231")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=AmazonS3_node1736049064231,
                                   ruleset=DEFAULT_DATA_QUALITY_RULESET,
                                   publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1736049060660", "enableDataQualityResultsPublishing": True},
                                   additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})

AmazonS3_node1736049066611 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1736049064231, connection_type="s3", format="csv",
                                                                          connection_options={"path": "s3://social-insights-data-atharv-01/tranformed/", "compression": "snappy", "partitionKeys": []},
                                                                          transformation_ctx="AmazonS3_node1736049066611")

job.commit()