import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


args = getResolvedOptions(sys.argv, ["JOB_NAME","target_database_name", "table_name"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

def replace(s):
    s = re.sub(r"[/_]", '', s)
    return s.lower()

database_name = args['target_database_name']
table_name = args["table_name"]

def malawi_ichis_expansion(database_name, table_name):
    # Script generated for node S3 bucket
    S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name,
        transformation_ctx="S3bucket_node1",
    )

    ApplyMapping_node2 = ApplyMapping.apply(
        frame=S3bucket_node1,
        mappings=[
            ("start_date", "string", "start_date", "string"),
            ("end_date", "string", "end_date", "string"),
            ("deviceid", "string", "deviceid", "string"),
            ("testtype", "string", "testtype", "string"),
            ("participant_id_number", "int", "participant_id_number", "string"),
            ("name_of_participant", "string", "name_of_participant", "string"),
            ("training_id_number", "string", "training_id_number", "string"),
            ("district_health_zone", "string", "district_health_zone", "string"),
            ("district", "string", "district", "string"),
            (
                "reporting_health_facility_name",
                "string",
                "reporting_health_facility_name",
                "string",
            ),
            ("position", "string", "position", "string"),
            ("qualification", "string", "qualification", "string"),
            ("age_category", "string", "age_category", "string"),
            (
                "reporting_catchment_area_name",
                "string",
                "reporting_catchment_area_name",
                "string",
            ),
            (
                "catchment_area_population_size",
                "string",
                "catchment_area_population_size",
                "string",
            ),
            (
                "years_worked_as_a_health_worker",
                "string",
                "years_worked_as_a_health_worker",
                "string",
            ),
            ("gender", "string", "gender", "string"),
            ("q1_score", "string", "q1_score", "string"),
            ("q3_score", "string", "q3_score", "string"),
            ("q4_score", "string", "q4_score", "string"),
            ("q5_score", "string", "q5_score", "string"),
            ("q6_score", "string", "q6_score", "string"),
            ("q8_score", "string", "q8_score", "string"),
            ("q10_score", "string", "q10_score", "string"),
            ("q7_score", "string", "q7_score", "string"),
            ("q8_score2", "string", "q8_score2", "string"),
            ("q9_score", "string", "q9_score", "string"),
            ("q10_score3", "string", "q10_score3", "string"),
            ("score", "int", "score", "string"),
            ("final_score", "string", "final_score", "string"),
            ("status", "string", "status", "string"),
        ],
        transformation_ctx="ApplyMapping_node2",
    )

    # Script generated for node Redshift Cluster
    RedshiftCluster_node3 = glueContext.write_dynamic_frame.from_catalog(
        frame=ApplyMapping_node2,
        database=database_name,
        table_name=table_name,
        redshift_tmp_dir=args["TempDir"],
        transformation_ctx="RedshiftCluster_node3",
    )

def sick_child(database_name, table_name):
    # Script generated for node Data Catalog table
    DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
        database=database_name,
        table_name=table_name,
        transformation_ctx="DataCatalogtable_node1",
    )

    # Script generated for node ApplyMapping
    ApplyMapping_node2 = ApplyMapping.apply(
        frame=DataCatalogtable_node1,
        mappings=[
            ("sickchildformid", "string", "sickchildformid", "string"),
            ("meta_autodate", "string", "meta_autodate", "string"),
            ("communityid", "string", "communityid", "string"),
            ("hhid", "string", "hhid", "string"),
            ("gender", "string", "gender", "string"),
            ("visittype", "string", "visittype", "string"),
            ("malaria_cases_treated", "string", "malaria_cases_treated", "string"),
            ("diarrhea_cases_treated", "int", "diarrhea_cases_treated", "int"),
            ("ari_cases_treated", "string", "ari_cases_treated", "string"),
            ("source", "string", "source", "string"),
        ],
        transformation_ctx="ApplyMapping_node2",
    )

    # Script generated for node Redshift Cluster
    RedshiftCluster_node3 = glueContext.write_dynamic_frame.from_catalog(
        frame=ApplyMapping_node2,
        database=database_name,
        table_name=table_name,
        redshift_tmp_dir=args["TempDir"],
        transformation_ctx="RedshiftCluster_node3",
    )

if replace(table_name).find('sickchild') != -1: sick_child(database_name, table_name)
if replace(table_name).find('ichisexpansion') != -1: malawi_ichis_expansion(database_name, table_name)


job.commit()
