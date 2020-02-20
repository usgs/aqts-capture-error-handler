import json
import os
import logging

from .sqs import send_message


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    logger.info(f'Event: {event}')

    queue_url = os.getenv('AWS_SQS_QUEUE_URL')
    region = os.getenv('AWS_DEPLOYMENT_REGION')

    send_message(
        queue_url=queue_url,
        message_body=json.dumps(event),
        region=region
    )
    return event
