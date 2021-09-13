from aws_cdk import (
    core as cdk,
    aws_s3 as s3,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_python as lambda_python,
    aws_dynamodb as dynamodb,
    aws_lambda_event_sources as lambda_event_sources,
    aws_logs as logs,
    aws_cloudwatch as cloudwatch,
)


class AwsCsvProcessorStack(cdk.Stack):
    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        csv_bucket = s3.Bucket(
            self,
            "csvBucket",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            bucket_name=f"csv-processor-bucket-{cdk.Aws.ACCOUNT_ID}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            enforce_ssl=True,
            auto_delete_objects=True,
        )

        # TODO Add alerting on basis of Lambda errors
        # TODO Add DLQ for events that could not be processed
        csv_processor_function = lambda_python.PythonFunction(
            self,
            "csvProcessor",
            function_name="csvProcessor",
            entry="./lambda_functions/csv_processor",
            index="processor.py",
            handler="handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            log_retention=logs.RetentionDays.ONE_WEEK,
        )

        # No action defined as I didn't have time to setup a proper alert receiver
        cloudwatch.Alarm(
            self,
            "csvProcessorAlarmErrors",
            metric=csv_processor_function.metric_errors(),
            evaluation_periods=1,
            threshold=0,
            actions_enabled=True,
            alarm_name="csv-processor-errors",
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            datapoints_to_alarm=1,
            period=cdk.Duration.seconds(60),
            statistic="Sum",
            treat_missing_data=cloudwatch.TreatMissingData.IGNORE,
        )

        csv_bucket_event_source = lambda_event_sources.S3EventSource(
            bucket=csv_bucket,
            events=[s3.EventType.OBJECT_CREATED],
            filters=[s3.NotificationKeyFilter(suffix=".csv")],
        )

        csv_processor_function.add_event_source(csv_bucket_event_source)

        csv_bucket.grant_read(csv_processor_function)

        csv_storage_table = dynamodb.Table(
            self,
            "csvStorageTable",
            table_name="csvStorageTable",
            partition_key=dynamodb.Attribute(
                name="customer_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            encryption=dynamodb.TableEncryption.AWS_MANAGED,
        )

        csv_storage_table.grant_read_write_data(csv_processor_function)

        csv_processor_function.add_environment(
            "TABLE_NAME", csv_storage_table.table_name
        )
