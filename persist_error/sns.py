"""
Module for sending a notification to SNS.

"""
import boto3


def send_notification(sns_arn, payload, region='us-west-2'):
    sns = boto3.client('sns', region_name=region)
    resp = sns.publish(
        TopicArn=sns_arn,
        Message=payload,
        Subject='Excessive Capture Failures'
    )
    return resp
