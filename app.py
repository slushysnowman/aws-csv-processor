#!/usr/bin/env python3
import os

from aws_cdk import core as cdk

from aws_csv_processor.aws_csv_processor_stack import AwsCsvProcessorStack


app = cdk.App()
AwsCsvProcessorStack(app, "AwsCsvProcessorStack")

app.synth()
