import json
import os
import logging
import warnings

from .sns import send_notification
from .sqs import send_message


logging.captureWarnings(capture=True)
log_level = os.getenv('LOG_LEVEL', 'ERROR')
logger = logging.getLogger()
logger.setLevel(log_level)


def lambda_handler(event, context):
    logger.info(f'Event: {event}')
    logger.info(f'Context: {context}')

    queue_url = os.getenv('AWS_SQS_QUEUE_URL')
    terminal_queue_url = os.getenv('AWS_TERMINAL_QUEUE_URL')
    sns_arn = os.getenv('AWS_SNS_ARN')
    region = os.getenv('AWS_DEPLOYMENT_REGION')
    deploy_stage = os.getenv('DEPLOY_STAGE')
    max_retries = int(os.getenv('MAX_RETRIES', 4))

    execution_arn = event['executionArn']
    initial_input = event['startInput']
    failure_cause = event['cause']
    logger.info(f'Initial input: {initial_input}; failure cause {failure_cause}')

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

        try:
            json_file = initial_input['Record']['s3']['object']['key']
            s3_bucket = initial_input['Record']['s3']['bucket']['name']
            s3_url = f'https://s3.console.aws.amazon.com/s3/object/{s3_bucket}?region={region}&prefix={json_file}'
        except KeyError:
            json_file = 'could not parse json file from state machine input'
            s3_url = 'json file could not be parsed from state machine input, no s3 url generated'

        try:
            json_file_size_mb = float(initial_input['Record']['s3']['object']['size']) * 10**-6
        except KeyError:
            json_file_size_mb = 'unknown'

        try:
            pretty_failure_cause = json.loads(failure_cause)
        except json.JSONDecodeError:
            pretty_failure_cause = failure_cause
        terminal_warning = {
            'message': 'Terminal Failure Warning',
            'file': {
                'name': json_file,
                'size': f'{json_file_size_mb} MB'
            },
            'execution_arn': execution_arn,
            'cause': pretty_failure_cause
        }
        warnings.warn(json.dumps(terminal_warning), UserWarning)

        # construct a logs insights query to view the entire state machine execution log
        execution_arn_replace_colons = execution_arn.replace(':', '*3a')
        step_function_log_group = f'step-functions-{deploy_stage.lower()}'
        time_interval_seconds = '86400'  # 1 day
        logs_insights_url = f'https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}#logsV2:logs-insights$3FqueryDetail$3D$257E$2528end$257E0$257Estart$257E-{time_interval_seconds}$257EtimeType$257E$2527RELATIVE$257Eunit$257E$2527seconds$257EeditorString$257E$2527fields*20*40timestamp*2c*20*40message*0a*7c*20filter*20*40message*20like*20*2f{execution_arn_replace_colons}*2f*0a*7c*20sort*20*40timestamp*20desc*0a*7c*20limit*2050*0a$257EisLiveTail$257Efalse$257EqueryId$257E$25276ae4a46e-cb22-461a-87a7-8d67fd2472e6$257Esource$257E$2528$257E$2527{step_function_log_group}$2529$2529\n'

        #  arn:aws:states:us-west-2:579777464052:express:aqts-capture-state-machine-PROD-EXTERNAL:1688f902-f2c7-4af3-a615-b5074db1b21f:db54eb4b-483b-4019-80aa-5281b5901501

        failure_message = (
            f'Step function execution {execution_arn} has terminally failed. \n'
            # TODO eventually we would like a link to the elasticsearch log from the failed lambda.  Minimally, we'll
            # TODO need to do IOW-729 first.
            f'The file we attempted to process: {s3_url} \n'
            f'This input has exceeded {max_retries} failures:\n'
            f'{json.dumps(initial_input, indent=4)}.\n'
            f'The execution reported this as the cause of the failure:\n'
            f'{failure_cause}.\n'
            f'To view the entire execution log: {logs_insights_url}\n'
            f'Please take a closer look at the underlying records and data.'
        )
        resp = send_notification(sns_arn, failure_message, subject_line=subject,)
        send_message(terminal_queue_url, failure_message)
        logger.info(f'Input failed more than {max_retries} times: {initial_input}. Notification sent to SNS: {resp}.')
    return initial_input


