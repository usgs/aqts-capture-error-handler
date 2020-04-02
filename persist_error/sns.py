"""
Module for sending a notification to SNS.

"""
import boto3


def send_notification(sns_arn, payload, region='us-west-2'):
    """
    Publish a message to SNS for subscribers to receive.

    :param str sns_arn: ARN of the SNS topic for the message
    :param str payload: message content
    :param str region: AWS region, defaults to us-west-2
    :return: SNS publish response
    :rtype: dict

    """
    sns = boto3.client('sns', region_name=region)
    resp = sns.publish(
        TopicArn=sns_arn,
        Message=payload,
        Subject=f'Excessive Capture Failures Reported'
    )
    return resp
