import json
import os
import logging

from .sfn import get_execution_history, find_root_failure_state
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

    execution_arn = event['executionArn']
    exec_history = get_execution_history(
        execution_arn=execution_arn,
        region=region
    )
    logger.info(f'Execution History: {exec_history}')

    failure_state = find_root_failure_state(exec_history)
    logger.info(f'Failure state: {failure_state}')

    try:
        failure_state['stepFunctionFails'] += 1  # increment number of failures
    except KeyError:
        failure_state['stepFunctionFails'] = 1  # start incrementing failures if this is first one
    logger.info(f'Incremented failure state: {failure_state}')

    if failure_state['stepFunctionFails'] <= 10:
        # send a message to SQS to restart the step function
        send_message(
            queue_url=queue_url,
            message_body=json.dumps(failure_state),
            region=region
        )
        logger.info(f'Failed event sent to {queue_url}.')
    else:
        # abort retry attempts if there are more than 10 failures
        # send a message to SNS for human to deal with it
        failure_message = (
            f'This input has caused 10 failures: {failure_state}.\n'
            f'Please take a closer look at the underlying records and data.'
        )
        resp = send_notification(sns_arn, failure_message)
        logger.info(f'This failed more than 10 times: {failure_state}. Notification sent to SNS: {resp}.')
    return failure_state
