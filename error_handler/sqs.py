"""
Module containing functions for persisting inputs that cause failures in for a particular state.

"""
import boto3


def send_message(queue_url, message_body, region='us-west-2'):
    sqs = boto3.client('sqs', region_name=region)
    resp = sqs.send_message(QueueUrl=queue_url, MessageBody=message_body)
    return resp
