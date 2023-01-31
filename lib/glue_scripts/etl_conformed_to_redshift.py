import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

def sick_child():
    # Script generated for node Data Catalog table
    DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
        database="lmd_datalake_conformed_arg",
        table_name="sickchild_data",
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
        database="test",
        table_name="lmd-20liberia_public_sick_child",
        redshift_tmp_dir=args["TempDir"],
        transformation_ctx="RedshiftCluster_node3",
    )

is_sick_child_job = False

if is_sick_child_job: 
    sick_child()
    
job.commit()
