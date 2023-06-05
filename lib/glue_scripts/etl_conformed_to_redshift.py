import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime
from pyspark.sql.functions import lit, col
from awsglue.dynamicframe import DynamicFrame
import boto3


args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "table_name", "target_databasename", "target_environment"])
# args = getResolvedOptions(
#     sys.argv, ["JOB_NAME"])


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


database = args['target_databasename']
table = str(args["table_name"])

# database = "malawi"
# table = "mlw_ichis_training"


curated_db_catalog = "lmd_datalake_conformed_arg"
current_date = datetime.datetime.now()


def pugify(s):
    s = re.sub(r"\s+", '', s)
    return s.strip().lower()


def get_key_value(obj, key):
    if obj[key] == "":
        raise "Object Key should be None not \"\" (empty)"
    return obj[key]


def filter_catalogue(kw, common_keys, haystack):
    try:
        next(hay for hay in haystack
             if all(pugify(str(get_key_value(hay, common_key))) == pugify(str(kw[common_key])) for common_key in common_keys))
        return False
    except StopIteration as si:
        return True


def get_common_keys(table):
    if table == 'mlw_ichis_training':
        return ['district', 'first_name', 'surname', 'commonly_used_phone_no']
    if table == 'sickchild':
        return ['name']
    return None


def secret():
    client = boto3.client('secretsmanager')
    get_secret_value_response = client.get_secret_value(
        SecretId=""
    )
    print(get_secret_value_response)


def load_redshift(catalogue_database, catalogue_table, database, table):

    data_catalogue_dynamicframe = glueContext.create_dynamic_frame.from_catalog(
        database=catalogue_database,
        table_name=catalogue_table,
        transformation_ctx="S3bucket_node1",
    )

    data_catalogue_df = data_catalogue_dynamicframe.toDF()
    old_catalogue_columns = data_catalogue_df.columns
    new_catalogue_columns = [column.lower() for column in old_catalogue_columns]

    column_name_mapping = dict(zip(old_catalogue_columns, new_catalogue_columns))

    data_catalogue_df_renamed = data_catalogue_df

    for old_name, new_name in column_name_mapping.items():
        data_catalogue_df_renamed = data_catalogue_df_renamed.withColumnRenamed(old_name, new_name)

    redshift_connection_options = {
        "url": "url/" + database,
        "dbtable": table,
        "user": "",
        "password": "",
        "redshiftTmpDir": args["TempDir"],
        "aws_iam_role": ""
    }

    redshift_dynamicframe = glueContext.create_dynamic_frame_from_options(
        "redshift", redshift_connection_options)

    redshift_df = redshift_dynamicframe.toDF()

    base_columns = data_catalogue_df_renamed.columns

    try:
        base_columns.remove('year')
        base_columns.remove('month')
        base_columns.remove('day')
    except ValueError as ve:
        print("Value does not exist " + str(ve))

    common_keys = get_common_keys(table)

    if common_keys is None:
        print("defaulting to fuzzy match for " + table)
        common_keys = list(set(base_columns).intersection(set(redshift_df.columns)))

    # print(id_list)

    for column in [column for column in redshift_df.columns if column not in data_catalogue_df_renamed.columns]:
        data_catalogue_df_renamed = data_catalogue_df_renamed.withColumn(column, lit(""))

    haystack = redshift_df.collect()

    load = data_catalogue_df_renamed.rdd.filter(lambda x: filter_catalogue(
        x, common_keys, haystack)).collect()

    to_redshift = spark.createDataFrame(load, data_catalogue_df_renamed.schema)

    to_redshift = to_redshift.withColumn("date_inserted", lit(
        current_date)).withColumn("last_update_date", lit(current_date)).drop(col("id"))

    data_catalogue_frame = DynamicFrame.fromDF(
        to_redshift, glueContext, "dataframecontext")

    ds0 = glueContext.write_dynamic_frame.from_options(frame=data_catalogue_frame.coalesce(1), connection_type="s3", format="csv", connection_options={
        "path": args["TempDir"], "partitionKeys": ["year", "month", "day"]}, transformation_ctx="ds0")

    print("Loading " + str(data_catalogue_frame.count()) + " Entries")
    print("Catalogue Count " + str(data_catalogue_df_renamed.count()) + " Entries")
    print("Redshift Entries " + str(redshift_dynamicframe.count()) + " Entries")
    print("Haystack " + str(len(haystack)) + " Entries")

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
    # secret()
except Exception as e:
    print(e)
    print("Error occured while loading data")
    raise e

job.commit()
