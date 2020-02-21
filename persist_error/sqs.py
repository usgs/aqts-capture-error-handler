"""
Module containing functions for persisting inputs that cause failures in for a particular state.

"""
import boto3


def send_message(queue_url, message_body, message_grp_id='step_function_error', region='us-west-2'):
    """
    Send a message to an SQS queue.

    :param str queue_url: http url of the SQS queue
    :param str message_body: the message body
    :param str message_grp_id: specifies that message belongs in a group in the FIFO queue
    :param str region: AWS region
    :return: message send response
    :rtype: dict

    """
    sqs = boto3.client('sqs', region_name=region)
    resp = sqs.send_message(QueueUrl=queue_url, MessageBody=message_body, MessageGroupId=message_grp_id)
    return resp
