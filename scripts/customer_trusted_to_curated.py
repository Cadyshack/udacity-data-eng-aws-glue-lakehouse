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

# Script generated for node Customer Trusted
CustomerTrusted_node1776205799653 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1776205799653")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1776205750053 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1776205750053")

# Script generated for node Join
Join_node1776205933938 = Join.apply(frame1=AccelerometerLanding_node1776205750053, frame2=CustomerTrusted_node1776205799653, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1776205933938")

# Script generated for node Drop Fields and Duplicates
SqlQuery1744 = '''
SELECT DISTINCT customername,
    email,
    phone,
    birthday,
    serialnumber,
    registrationdate,
    lastupdatedate,
    sharewithresearchasofdate,
    sharewithpublicasofdate,
    sharewithfriendsasofdate
FROM myDataSource;
'''
DropFieldsandDuplicates_node1776206519476 = sparkSqlQuery(glueContext, query = SqlQuery1744, mapping = {"myDataSource":Join_node1776205933938}, transformation_ctx = "DropFieldsandDuplicates_node1776206519476")

# Script generated for node Customer Curated
CustomerCurated_node1776206202315 = glueContext.getSink(path="s3://christian-data-lakehouse/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1776206202315")
CustomerCurated_node1776206202315.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1776206202315.setFormat("json")
CustomerCurated_node1776206202315.writeFrame(DropFieldsandDuplicates_node1776206519476)
job.commit()