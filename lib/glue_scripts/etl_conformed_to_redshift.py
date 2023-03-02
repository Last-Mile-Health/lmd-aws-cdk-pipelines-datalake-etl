import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime
from pyspark.sql.functions import lit
from awsglue.dynamicframe import DynamicFrame



args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "table_name", "target_databasename"])
    
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def replace(s):
    s = re.sub(r"[/_]", '', s)
    return s.lower()


database = args['target_databasename']
table = str(args["table_name"])

def load_redshift(catalogue_database, catalogue_table, database, table):
    
    data_catalogue_frame = glueContext.create_dynamic_frame.from_catalog(
        database=catalogue_database,
        table_name=catalogue_table,
        transformation_ctx="S3bucket_node1",
    )
    
    df = data_catalogue_frame.toDF().withColumn("date_inserted", lit(datetime.datetime.now()))
    
    data_catalogue_frame = DynamicFrame.fromDF(df, glueContext, "dfdynf")
    
    redshift_load_dyf = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=data_catalogue_frame,
        catalog_connection="redshift-connection",
        connection_options={"dbtable": table, "database": database},
        redshift_tmp_dir=args["TempDir"],
        transformation_ctx="redshift_load_dyf"
    )

if replace(table).find('sickchild') != -1:
    load_redshift("liberia", "sickchild_data", "liberia", "sickchild_data")    
if replace(table).find('routinevisit') != -1:
    load_redshift("liberia", "routinevisit", "liberia", "routinevisit")
if replace(table).find('ichisexpansion') != -1:
    load_redshift("lmd_datalake_conformed_arg", "mlw_ichis_expansion", "malawi", "mlw_ichis_expansion")

job.commit()