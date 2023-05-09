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

curated_db_catalog = "lmd_datalake_conformed_arg"
current_date = datetime.datetime.now()


def load_redshift(catalogue_database, catalogue_table, database, table):

    data_catalogue_frame = glueContext.create_dynamic_frame.from_catalog(
        database=catalogue_database,
        table_name=catalogue_table,
        transformation_ctx="S3bucket_node1",
    )

    df = data_catalogue_frame.toDF().withColumn("date_inserted", lit(
        current_date)).withColumn("last_update_date", lit(current_date))

    data_catalogue_frame = DynamicFrame.fromDF(
        df, glueContext, "dataframecontext")

    redshift_load_dyf = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=data_catalogue_frame,
        catalog_connection="redshift-connection",
        connection_options={"dbtable": table, "database": database},
        redshift_tmp_dir=args["TempDir"],
        transformation_ctx="redshift_load_dyf"
    )


try:
    load_redshift(curated_db_catalog, table,
                  database, table)
except Exception as e:
    print(e)
    print("Error occured while loading data")

job.commit()
