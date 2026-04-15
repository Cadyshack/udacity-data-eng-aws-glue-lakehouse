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

# Script generated for node Customer Landing
CustomerLanding_node1775682751644 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://christian-data-lakehouse/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1775682751644")

# Script generated for node Share with Research
SqlQuery885 = '''
select * from myDataSource
WHERE sharewithresearchasofdate is NOT NULL;
'''
SharewithResearch_node1775683558091 = sparkSqlQuery(glueContext, query = SqlQuery885, mapping = {"myDataSource":CustomerLanding_node1775682751644}, transformation_ctx = "SharewithResearch_node1775683558091")

# Script generated for node Customer Trusted
CustomerTrusted_node1775684033460 = glueContext.getSink(path="s3://christian-data-lakehouse/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1775684033460")
CustomerTrusted_node1775684033460.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1775684033460.setFormat("json")
CustomerTrusted_node1775684033460.writeFrame(SharewithResearch_node1775683558091)
job.commit()