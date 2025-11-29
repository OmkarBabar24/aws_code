import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
s3_path = "s3://omkar-demo-s1/model_auth_Rep/"

# datasource = glueContext.create_dynamic_frame.from_options(
#     format_options={
#         "separator": ",",
#         "quoteChar": "\"",
#         "escaper": "\\",
#         "withHeader": True,
#         "optimizePerformance": True
#     },
#     connection_type="s3",
#     format="csv",
#     connection_options={"paths": [s3_path], "recurse": True},
#     transformation_ctx="datasource"
# )

# # Convert to DataFrame if you want to use Spark operations
# df = datasource.toDF()

# df.show(5)
df=spark.read.option("header","True").csv(s3_path)
df.show()
job.commit()
job.commit()