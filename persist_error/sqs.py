"""
Module containing functions for persisting inputs that cause failures in for a particular state.

"""
import uuid

import boto3


def send_message(queue_url, message_body, message_grp_id='step_function_error', region='us-west-2'):
    """
    Send a message to an SQS FIFO queue.

    :param str queue_url: http url of the SQS queue
    :param str message_body: the message body
    :param str dedup_id: string use for deduplication of messages in the SQS FIFO queue
    :param str message_grp_id: specifies that message belongs in a group in the FIFO queue
    :param str region: AWS region
    :return: message send response
    :rtype: dict

    """
    sqs = boto3.client('sqs', region_name=region)

    deduplication_id = str(uuid.uuid4())

    resp = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=message_body,
        MessageDeduplicationId=deduplication_id,
        MessageGroupId=message_grp_id
    )
    return resp
