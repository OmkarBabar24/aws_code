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
# Read CSV files from S3
# s3_path = "# Read CSV files from S3
s1_path = "s3://expo-demo-12/project-01/Project 2/drivers.csv/"
s2_path = "s3://expo-demo-12/project-01/Project 2/routes.csv"
s3_path = "s3://expo-demo-12/project-01/Project 2/trips.csv/"
s4_path = "s3://expo-demo-12/project-01/Project 2/vehicles.csv"

df1=spark.read.option("header","True").csv(s1_path)
df2=spark.read.option("header","True").csv(s2_path)
df3=spark.read.option("header","True").csv(s3_path)
df4=spark.read.option("header","True").csv(s4_path)

df1.show()
df2.show()
df3.show()
df4.show()
job.commit()
