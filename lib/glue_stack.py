# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import aws_cdk.core as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_glue as glue
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kms as kms
import aws_cdk.aws_s3_deployment as s3_deployment

from .configuration import (
    AVAILABILITY_ZONE_1, SUBNET_ID_1,
    S3_ACCESS_LOG_BUCKET, S3_KMS_KEY, S3_CONFORMED_BUCKET, S3_PURPOSE_BUILT_BUCKET, SHARED_SECURITY_GROUP_ID,
    ACCOUNT_ID, REGION, get_environment_configuration, get_logical_id_prefix, get_resource_name_prefix
)


class GlueStack(cdk.Stack):
    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        target_environment: str,
        **kwargs
    ) -> None:
        """
        CloudFormation stack to create Glue Jobs, Connections,
        Script Bucket, Temporary Bucket, and an IAM Role for permissions.

        @param scope cdk.Construct: Parent of this stack, usually an App or a Stage, but could be any construct.
        @param construct_id str:
            The construct ID of this stack. If stackName is not explicitly defined,
            this id (and any parent IDs) will be used to determine the physical ID of the stack.
        @param target_environment str: The target environment for stacks in the deploy stage
        @param kwargs:
        """
        super().__init__(scope, construct_id, **kwargs)

        self.mappings = get_environment_configuration(target_environment)
        logical_id_prefix = get_logical_id_prefix()
        resource_name_prefix = get_resource_name_prefix()

        existing_access_logs_bucket_name = cdk.Fn.import_value(self.mappings[S3_ACCESS_LOG_BUCKET])
        access_logs_bucket = s3.Bucket.from_bucket_attributes(
            self,
            'ImportedBucket',
            bucket_name=existing_access_logs_bucket_name
        )
        s3_kms_key_parameter = cdk.Fn.import_value(self.mappings[S3_KMS_KEY])
        s3_kms_key = kms.Key.from_key_arn(self, 'ImportedKmsKey', s3_kms_key_parameter)
        shared_security_group_parameter = cdk.Fn.import_value(self.mappings[SHARED_SECURITY_GROUP_ID])
        glue_connection_subnet = cdk.Fn.import_value(self.mappings[SUBNET_ID_1])
        glue_connection_availability_zone = cdk.Fn.import_value(self.mappings[AVAILABILITY_ZONE_1])

        conformed_bucket_name = cdk.Fn.import_value(self.mappings[S3_CONFORMED_BUCKET])
        conformed_bucket = s3.Bucket.from_bucket_name(
            self,
            id='ImportedConformedBucket',
            bucket_name=conformed_bucket_name
        )
        purposebuilt_bucket_name = cdk.Fn.import_value(self.mappings[S3_PURPOSE_BUILT_BUCKET])
        purposebuilt_bucket = s3.Bucket.from_bucket_name(
            self,
            id='ImportedPurposeBuiltBucket',
            bucket_name=purposebuilt_bucket_name
        )
        shared_security_group = ec2.SecurityGroup.from_security_group_id(
            self,
            'ImportedSecurityGroup',
            shared_security_group_parameter
        )
        subnet = ec2.Subnet.from_subnet_attributes(
            self,
            'ImportedSubnet',
            subnet_id=glue_connection_subnet,
            availability_zone=glue_connection_availability_zone
        )
        glue_scripts_bucket = self.glue_scripts_bucket(
            target_environment,
            logical_id_prefix,
            resource_name_prefix,
            s3_kms_key,
            access_logs_bucket
        )
        glue_scripts_temp_bucket = self.glue_scripts_temporary_bucket(
            target_environment,
            logical_id_prefix,
            resource_name_prefix,
            s3_kms_key,
            access_logs_bucket
        )
        glue_role = self.get_role(
            target_environment,
            logical_id_prefix,
            resource_name_prefix,
            s3_kms_key,
        )

        job_connection = glue.Connection(
            self,
            f'{target_environment}{logical_id_prefix}RawToConformedWorkflowConnection',
            type=glue.ConnectionType.NETWORK,
            connection_name=f'{target_environment.lower()}-{resource_name_prefix}-raw-to-conformed-connection',
            security_groups=[shared_security_group],
            subnet=subnet
        )

        self.raw_to_conformed_job = glue.CfnJob(
            self,
            f'{target_environment}{logical_id_prefix}RawToConformedJob',
            name=f'{target_environment.lower()}-{resource_name_prefix}-raw-to-conformed-job',
            command=glue.CfnJob.JobCommandProperty(
                name='glueetl',
                python_version='3',
                script_location=f's3://{glue_scripts_bucket.bucket_name}/etl/etl_raw_to_conformed.py'
            ),
            connections=glue.CfnJob.ConnectionsListProperty(
                connections=[job_connection.connection_name],
            ),
            default_arguments={
                '--enable-glue-datacatalog': '""',
                '--target_database_name': 'lmd_datalake_arg',
                '--target_bucket': conformed_bucket.bucket_name,
                '--target_table_name': 'lmd_datalake_raw',
                '--TempDir': f's3://{glue_scripts_temp_bucket.bucket_name}/etl/raw-to-conformed',
            },
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=10,
            ),
            glue_version='3.0',
            max_retries=2,
            number_of_workers=20,
            role=glue_role.role_arn,
            worker_type='Standard',
        )

        self.conformed_to_purpose_built_job = glue.CfnJob(
            self,
            f'{target_environment}{logical_id_prefix}ConformedToPurposeBuiltJob',
            name=f'{target_environment.lower()}-{resource_name_prefix}-conformed-to-purpose-built-job',
            command=glue.CfnJob.JobCommandProperty(
                name='glueetl',
                python_version='3',
                script_location=f's3://{glue_scripts_bucket.bucket_name}/etl/etl_conformed_to_purposebuilt.py'
            ),
            connections=glue.CfnJob.ConnectionsListProperty(
                connections=[job_connection.connection_name],
            ),
            default_arguments={
                '--enable-glue-datacatalog': '""',
                '--target_database_name': 'lmd_datalake_conformed_arg',
                '--target_bucketname': purposebuilt_bucket.bucket_name,
                '--target_table_name': 'lmd_datalake_conformed',
                '--txn_bucket_name': glue_scripts_bucket.bucket_name,
                '--txn_sql_prefix_path': '/etl/transformation-sql/',
                '--TempDir': f's3://{glue_scripts_temp_bucket.bucket_name}/etl/conformed-to-purpose-built'
            },
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=10,
            ),
            glue_version='3.0',
            max_retries=2,
            number_of_workers=20,
            role=glue_role.role_arn,
            worker_type='Standard',
        )

        self.conformed_to_redshift_job = glue.CfnJob(
            self,
            f'{target_environment}{logical_id_prefix}ConformedToRedshiftJob',
            name=f'{target_environment.lower()}-{resource_name_prefix}-conformed-to-redshift-job',
            description="Glue Job to ingest PARQUET file data from S3 to RedshiftServerless",
            role=glue_role.role_arn,
            glue_version="3.0",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{glue_scripts_bucket.bucket_name}/etl/etl_conformed_to_redshift.py",
                python_version="3"
            ),
            default_arguments={
                "--enable-metrics": True,
                "--enable-continuous-cloudwatch-log": True,
                "--job-bookmark-option": "job-bookmark-enable",
                '--TempDir': f's3://{glue_scripts_temp_bucket.bucket_name}/etl/conformed-to-redshift',
                '--enable-spark-ui': True,
                '--enable-job-insights': True,
                '--enable-glue-datacatalog': True,
                '--job-language': 'python',
                '--additional-python-modules': 'botocore>=1.29.147,boto3>=1.26.147',
                '--workgroup_name': f"{target_environment}-lmd-v2".lower(),
                '--region': self.mappings[REGION],
                '--account_id': self.mappings[ACCOUNT_ID]
            },
            max_retries=2,
            worker_type='Standard',
            number_of_workers=20,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=10)
        )

    def glue_scripts_bucket(
        self,
        target_environment,
        logical_id_prefix: str,
        resource_name_prefix: str,
        s3_kms_key: kms.Key,
        access_logs_bucket: s3.Bucket
    ) -> s3.Bucket:
        """
        Creates S3 Bucket that contains glue scripts used in Job execution

        @param target_environment str: The target environment for stacks in the deploy stage
        @param logical_id_prefix str: The logical id prefix to apply to all CloudFormation resources
        @param resource_name_prefix str: The prefix applied to all resource names
        @param s3_kms_key kms.Key: The KMS Key to use for encryption of data at rest
        @param access_logs_bucket s3.Bucket: The access logs target for this bucket
        """
        bucket_name = f'{target_environment.lower()}-{resource_name_prefix}-{self.account}-etl-scripts'
        bucket = s3.Bucket(
            self,
            f'{target_environment}{logical_id_prefix}RawGlueScriptsBucket',
            bucket_name=bucket_name,
            access_control=s3.BucketAccessControl.PRIVATE,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            bucket_key_enabled=s3_kms_key is not None,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=s3_kms_key,
            public_read_access=False,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            versioned=True,
            object_ownership=s3.ObjectOwnership.OBJECT_WRITER,
            server_access_logs_bucket=access_logs_bucket,
            server_access_logs_prefix=bucket_name,
        )
        # Dynamically upload resources to the script target
        s3_deployment.BucketDeployment(
            self,
            'DeployGlueJobScript',
            # This path is relative to the root of the project
            sources=[s3_deployment.Source.asset('./lib/glue_scripts')],
            destination_bucket=bucket,
            destination_key_prefix='etl',
        )

        return bucket

    def glue_scripts_temporary_bucket(
        self, target_environment, logical_id_prefix: str, resource_name_prefix: str,
        s3_kms_key: kms.Key, access_logs_bucket: s3.Bucket
    ) -> s3.Bucket:
        """
        Creates S3 Bucket used as a temporary file store in Job execution

        @param target_environment str: The target environment for stacks in the deploy stage
        @param logical_id_prefix str: The logical id prefix to apply to all CloudFormation resources
        @param resource_name_prefix str: The prefix applied to all resource names
        @param s3_kms_key kms.Key: The KMS Key to use for encryption of data at rest
        @param access_logs_bucket s3.Bucket: The access logs target for this bucket
        """
        bucket_name = f'{target_environment.lower()}-{resource_name_prefix}-{self.account}-glue-temporary-scripts'
        bucket = s3.Bucket(
            self,
            f'{target_environment}{logical_id_prefix}RawGlueScriptsTemporaryBucket',
            bucket_name=bucket_name,
            access_control=s3.BucketAccessControl.PRIVATE,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            bucket_key_enabled=s3_kms_key is not None,
            encryption=s3.BucketEncryption.KMS if s3_kms_key else s3.BucketEncryption.S3_MANAGED,
            encryption_key=s3_kms_key if s3_kms_key else None,
            public_read_access=False,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            versioned=True,
            object_ownership=s3.ObjectOwnership.OBJECT_WRITER,
            server_access_logs_bucket=access_logs_bucket,
            server_access_logs_prefix=bucket_name,
        )

        return bucket

    def get_role(
        self,
        target_environment: str,
        logical_id_prefix: str,
        resource_name_prefix: str,
        s3_kms_key: kms.Key,
    ) -> iam.Role:
        """
        Creates the role used during Glue Job execution

        @param target_environment str: The target environment for stacks in the deploy stage
        @param logical_id_prefix str: The logical id prefix to apply to all CloudFormation resources
        @param resource_name_prefix str: The prefix applied to all resource names
        @param s3_kms_key kms.Key: The KMS Key to provide permissions to

        @returns iam.Role: The role that was created
        """
        return iam.Role(
            self,
            f'{target_environment}{logical_id_prefix}RawGlueRole',
            role_name=f'{target_environment.lower()}-{resource_name_prefix}-raw-glue-role',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            inline_policies={
                'AllowS3GetActions': iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                's3:ListBucketVersions',
                                's3:ListBucket',
                                's3:GetBucketNotification',
                                's3:GetBucketLocation',
                            ],
                            resources=[
                                'arn:aws:s3:::*'
                            ]
                        )
                    ]),
                'AllowS3PutActions': iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                's3:ReplicationObject',
                                's3:PutObject',
                                's3:GetObject',
                                's3:DeleteObject',
                            ],
                            resources=[
                                'arn:aws:s3:::*/*'
                            ]
                        )
                    ]),
                'AllowS3ListAllActions': iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                's3:ListAllMyBuckets',
                            ],
                            resources=[
                                '*'
                            ]
                        )
                    ]),
                'AllowKMSActions': iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                'kms:*',
                            ],
                            resources=[
                                s3_kms_key.key_arn,
                            ]
                        )
                    ]),
            },
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftAllCommandsFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("SecretsManagerReadWrite"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftFullAccess")
            ]
        )