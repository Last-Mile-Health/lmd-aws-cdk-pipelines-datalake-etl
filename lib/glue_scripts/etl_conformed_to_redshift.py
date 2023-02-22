import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


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
table = str(args["table_name"]).lower()


def load_redshift(database, table):
    data_catalogue_frame = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table,
        transformation_ctx="S3bucket_node1",
    )

    redshift_load_dyf = glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=data_catalogue_frame,
        catalog_connection="lmd-20",
        connection_options={"dbtable": table, "database": database},
        redshift_tmp_dir=args["TempDir"],
        transformation_ctx="redshift_load_dyf"
    )

job.commit()