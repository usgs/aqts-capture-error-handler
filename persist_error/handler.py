import json
import os
import logging

from .sns import send_notification
from .sqs import send_message


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    logger.info(f'Event: {event}')
    logger.info(f'Context: {context}')

    queue_url = os.getenv('AWS_SQS_QUEUE_URL')
    sns_arn = os.getenv('AWS_SNS_ARN')
    region = os.getenv('AWS_DEPLOYMENT_REGION')
    max_retries = int(os.getenv('MAX_RETRIES', 4))

    execution_arn = event['executionArn']
    initial_input = event['startInput']
    logger.info(f'Initial input: {initial_input}')

    try:
        initial_input['stepFunctionFails'] += 1
    except KeyError:
        initial_input['stepFunctionFails'] = 1

    try:
        initial_input['previousExecutions'].append(execution_arn)
    except KeyError:
        initial_input['previousExecutions'] = [execution_arn]

    if initial_input['stepFunctionFails'] <= max_retries:
        # send a message to SQS to restart the step function
        send_message(
            queue_url=queue_url,
            message_body=json.dumps(initial_input),
            region=region
        )
        logger.info(f'Failed event sent to {queue_url}.')
    else:
        # abort retry attempts if there more failures than the set condition
        # send a message to SNS for human to deal with it
        subject = 'Excessive Capture Failures Reported'
        failure_message = (
            f'Step function execution {execution_arn} has terminally failed. '
            f'This input has exceeded {max_retries} failures for an individual state: {input}.\n'
            f'Please take a closer look at the underlying records and data.'
        )
        resp = send_notification(sns_arn, failure_message, subject_line=subject,)
        logger.info(f'Input failed more than {max_retries} times: {initial_input}. Notification sent to SNS: {resp}.')
    return initial_input
