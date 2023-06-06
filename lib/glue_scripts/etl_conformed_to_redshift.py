import sys
import re
import json
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
    sys.argv, ["JOB_NAME", "region", "workgroup_name", "account_id", "table_name", "target_databasename",])


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


database = args['target_databasename']
table = str(args["table_name"])
region = args["region"]
workgroup_name = args["workgroup_name"]
account_id = args["account_id"]

curated_db_catalog = "lmd_datalake_conformed_arg"
current_date = datetime.datetime.now()


def pugify(s):
    s = re.sub(r"\s+", '', s)
    return s.strip().lower()


def get_key_value(obj, key):
    if obj[key] == "":
        raise "Object Key should be None not \"\" (empty)"
    return obj[key]


# match object again datawarehouse entries across dynamic keys
def filter_catalogue(kw, common_keys, haystack):
    try:
        next(hay for hay in haystack
             if all(pugify(str(get_key_value(hay, common_key))) == pugify(str(kw[common_key])) for common_key in common_keys))
        return False
    except StopIteration as si:
        return True


def get_common_keys(table):
    # should a table be specific on fields
    # to match against, use this function to return a list of such fields
    # sample field selection for mlw_ichis_table
    # if table == 'mlw_ichis_training':
    #     return ['district', 'first_name', 'surname', 'commonly_used_phone_no']
    return None


# Fetch passwod from secrets manager - lmdredshiftpassword match
def get_password():
    client = boto3.client('secretsmanager')
    secrets = []
    response = client.list_secrets()
    secrets.extend(response['SecretList'])
    while 'NextToken' in response:
        response = client.list_secrets(NextToken=response['NextToken'])
        secrets.extend(response['SecretList'])
    redshift_password = next(secret for secret in secrets if 'lmdredshiftpassword' in secret['Name'].strip().lower())
    get_secret_value_response = client.get_secret_value(
        SecretId=redshift_password['Name']
    )
    return get_secret_value_response['SecretString']


# Fetch namespace information (db user & role arn)
def get_namespace_info():
    client = boto3.client('redshift-serverless')
    namespaces = []
    response = client.list_namespaces()
    namespaces.extend(response['namespaces'])
    while 'NextToken' in response:
        response = client.list_secrets(NextToken=response['NextToken'])
        namespaces.extend(response['namespaces'])
    namespace = next(namespace for namespace in namespaces if 'lmd-v2' in namespace['namespaceName'].lower())
    return (namespace['adminUsername'], namespace['defaultIamRoleArn'])


def load_redshift(catalogue_database, catalogue_table, database, table):

    data_catalogue_dynamicframe = glueContext.create_dynamic_frame.from_catalog(
        database=catalogue_database,
        table_name=catalogue_table,
        transformation_ctx="S3bucket_node1",
    )

    data_catalogue_df = data_catalogue_dynamicframe.toDF()
    old_catalogue_columns = data_catalogue_df.columns
    new_catalogue_columns = [column.lower() for column in old_catalogue_columns]

    # Rename columns to lower case underscore equivalent
    column_name_mapping = dict(zip(old_catalogue_columns, new_catalogue_columns))

    data_catalogue_df_renamed = data_catalogue_df

    for old_name, new_name in column_name_mapping.items():
        data_catalogue_df_renamed = data_catalogue_df_renamed.withColumnRenamed(old_name, new_name)

    db_username, default_redshift_role_arn = get_namespace_info()
    jdbc_connection = f"jdbc:redshift://{workgroup_name}.{account_id}.{region}.redshift-serverless.amazonaws.com:5439/{database}"
    redshift_password = get_password()

    redshift_connection_options = {
        "url": jdbc_connection,
        "dbtable": table,
        "user": db_username,
        "password": redshift_password,
        "redshiftTmpDir": args["TempDir"],
        "aws_iam_role": default_redshift_role_arn
    }

    redshift_dynamicframe = glueContext.create_dynamic_frame_from_options(
        "redshift", redshift_connection_options)

    redshift_df = redshift_dynamicframe.toDF()

    # Capture catalogue columns before updating redshift columns
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

    # Update catalogue dataframe with redshift columns (schema)
    for column in [column for column in redshift_df.columns if column not in data_catalogue_df_renamed.columns]:
        data_catalogue_df_renamed = data_catalogue_df_renamed.withColumn(column, lit(""))

    haystack = redshift_df.collect()

    # Filter catalogue entries based on predicate
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

    redshift_load_dyf = glueContext.write_dynamic_frame.from_options(
        frame=data_catalogue_frame,
        connection_type="redshift",
        connection_options={
            "url": jdbc_connection,
            "dbtable": table,
            "user": db_username,
            "password": redshift_password,
            "redshiftTmpDir": args["TempDir"]
        },
        transformation_ctx="redshift_load_dyf"
    )


try:
    load_redshift(curated_db_catalog, table,
                  database, table)
except Exception as e:
    print(e)
    print("Error occured while loading data")
    raise e

job.commit()
