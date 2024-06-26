# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from boto3.dynamodb.conditions import Key
from pyspark.sql import HiveContext
from awsglue.dynamicframe import DynamicFrame

from pyspark.conf import SparkConf

from datetime import datetime
import botocore


# @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'txn_bucket_name',
        'txn_sql_prefix_path',
        'target_database_name',
        'target_bucketname',
        'base_file_name',
        'p_year',
        'p_month',
        'p_day',
        'table_name'
    ]
)

conf = SparkConf()
conf.set("spark.sql.parquet.enableVectorizedReader", "false")

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
sqlContext = HiveContext(sc)


def table_exists(target_database, table_name):
    """
    Function to check if table exists returns true/false
    @param target_database:
    @param table_name:
    """
    try:
        glue_client = boto3.client("glue")
        glue_client.get_table(DatabaseName=target_database, Name=table_name)

        return True
    except:
        return False


def create_database():
    """
    Function to create catalog database if does not exists
    """
    response = None

    glue_client = boto3.client('glue')
    database_name = args['target_database_name']

    try:
        # global response
        response = glue_client.get_database(
            Name=database_name
        )
        print(response)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'EntityNotFoundException':
            print('The requested database: {0} was not found.'.format(database_name))
        else:
            print('Error Message: {}'.format(error.response['Error']['Message']))

    if response is None:
        print('I am going to create a database')
        response = glue_client.create_database(
            DatabaseInput={
                'Name': database_name
            }
        )
        print("create_database_response: ", response)


def upsert_catalog_table(df, target_database, table_name, classification, storage_location):
    """
    Function to upsert catalog table
    @param df:
    @param target_database:
    @param table_name:
    @param classification:
    @param storage_location:
    """
    create_database()
    df_schema = df.dtypes
    schema = []

    for s in df_schema:
        if (s[0] != 'year' and s[0] != 'month' and s[0] != 'day'):
            if s[1] == "decimal(10,0)":
                v = {"Name": s[0], "Type": "int"}
            elif s[1] == "null":
                v = {"Name": s[0], "Type": "string"}
            else:
                v = {"Name": s[0], "Type": s[1]}
            schema.append(v)

    print(schema)
    input_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    output_format = 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    serde_info = {
        'Parameters': {
            'serialization.format': '1'
        },
        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    }
    storage_descriptor = {
        'Columns': schema,
        'InputFormat': input_format,
        'OutputFormat': output_format,
        'SerdeInfo': serde_info,
        'Location': storage_location
    }

    partition_key = [
        {'Name': 'year', 'Type': 'string'},
        {'Name': 'month', 'Type': 'string'},
        {'Name': 'day', 'Type': 'string'}
    ]
    table_input = {
        'Name': table_name,
        'StorageDescriptor': storage_descriptor,
        'Parameters': {
            'classification': classification,
            'SourceType': 's3',
            'SourceTableName': table_name,
            'TableVersion': '0'
        },
        'TableType': 'EXTERNAL_TABLE',
        'PartitionKeys': partition_key
    }

    try:
        glue_client = boto3.client('glue')
        if not table_exists(target_database, table_name):
            print('[INFO] Target Table name: {} does not exist.'.format(table_name))
            glue_client.create_table(DatabaseName=target_database, TableInput=table_input)
        else:
            print('[INFO] Trying to update: TargetTable: {}'.format(table_name))
            glue_client.update_table(DatabaseName=target_database, TableInput=table_input)
    except botocore.exceptions.ClientError as error:
        print('[ERROR] Glue job client process failed:{}'.format(error))
        raise error
    except Exception as e:
        print('[ERROR] Glue job function call failed:{}'.format(e))
        raise e


def add_partition(rec):
    """
    Function to add partition
    """
    partition_path = f'{args["p_year"]}/{args["p_month"]}/{args["p_day"]}'
    rec['year'] = args['p_year']
    rec['month'] = args['p_month']
    rec['day'] = args['p_day']
    print(partition_path)

    return rec


def main():
    prefix = args['txn_sql_prefix_path']
    key = prefix[1:] + args['table_name'] + '.sql'

    print(key)

    try:
        s3 = boto3.client('s3')
        response = s3.get_object(
            Bucket=args['txn_bucket_name'],
            Key=key
        )
    except botocore.exceptions.ClientError as error:
        print('[ERROR] Glue job client process failed:{}'.format(error))
        raise error
    except Exception as e:
        print('[ERROR] Glue job function call failed:{}'.format(e))
        raise e

    df = spark.sql(response['Body'].read().decode('utf-8'))

    target_s3_location = "s3://" + args['target_bucketname'] + "/datalake/"
    storage_location = target_s3_location + args['table_name']
    upsert_catalog_table(df, args['target_database_name'], args['table_name'], 'PARQUET', storage_location)

    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    spark.conf.set('hive.exec.dynamic.partition', 'true')
    spark.conf.set('hive.exec.dynamic.partition.mode', 'nonstrict')

    df.write.partitionBy('year', 'month', 'day').format('parquet').save(storage_location, mode='overwrite')

    target_table_name = args['target_database_name'] + '.' + args['table_name']
    spark.sql(f'ALTER TABLE {target_table_name} RECOVER PARTITIONS')


if __name__ == '__main__':
    main()
