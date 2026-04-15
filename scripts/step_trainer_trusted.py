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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1776213501591 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1776213501591")

# Script generated for node Customer Curated
CustomerCurated_node1776213543743 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1776213543743")

# Script generated for node Filter Step Trainer Trusted
SqlQuery1617 = '''
SELECT sensorreadingtime, s.serialnumber, distancefromobject
FROM STLanding AS s
JOIN CCSource AS c
ON s.serialnumber = c.serialnumber;
'''
FilterStepTrainerTrusted_node1776213597972 = sparkSqlQuery(glueContext, query = SqlQuery1617, mapping = {"STLanding":StepTrainerLanding_node1776213501591, "CCSource":CustomerCurated_node1776213543743}, transformation_ctx = "FilterStepTrainerTrusted_node1776213597972")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1776214820113 = glueContext.getSink(path="s3://christian-data-lakehouse/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1776214820113")
StepTrainerTrusted_node1776214820113.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1776214820113.setFormat("json")
StepTrainerTrusted_node1776214820113.writeFrame(FilterStepTrainerTrusted_node1776213597972)
job.commit()