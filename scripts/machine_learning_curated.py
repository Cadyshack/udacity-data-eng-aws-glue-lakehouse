import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1776290177977 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1776290177977")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1776290175817 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1776290175817")

# Script generated for node Join Accelerometer to Step Trainer
SqlQuery2196 = '''
SELECT st.sensorreadingtime,
		st.serialnumber,
		st.distancefromobject,
		at.user,
		at.x,
		at.y,
		at.z
FROM at
JOIN st
ON at.timestamp = st.sensorreadingtime;
'''
JoinAccelerometertoStepTrainer_node1776290296509 = sparkSqlQuery(glueContext, query = SqlQuery2196, mapping = {"st":StepTrainerTrusted_node1776290175817, "at":AccelerometerTrusted_node1776290177977}, transformation_ctx = "JoinAccelerometertoStepTrainer_node1776290296509")

# Script generated for node Amazon S3
AmazonS3_node1776290498061 = glueContext.getSink(path="s3://christian-data-lakehouse/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1776290498061")
AmazonS3_node1776290498061.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1776290498061.setFormat("json")
AmazonS3_node1776290498061.writeFrame(JoinAccelerometertoStepTrainer_node1776290296509)
job.commit()