"""
Module for working with the AWS Step Functions API

"""
import boto3


def get_execution_history(execution_arn, region='us-west-2'):
    sfn = boto3.client('sfn', region_name=region)
    history = sfn.get_execution_history(executionArn=execution_arn)
    return history
