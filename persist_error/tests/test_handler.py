"""
Tests for the AWS Lambda handler.

"""
import json
from unittest import TestCase, mock
import warnings

from ..handler import lambda_handler


class TestLambdaHandler(TestCase):

    queue_url = 'https://sqs.us-south-10.amazonaws.com/887501/some-queue-name'
    sns_arn = 'arn:aws:sns:us-south-23:5746521541:fake-notification'
    region = 'us-south-10'
    max_retries = 6
    mock_env_vars = {
        'AWS_SQS_QUEUE_URL': queue_url,
        'AWS_SNS_ARN': sns_arn,
        'AWS_DEPLOYMENT_REGION': region,
        'MAX_RETRIES': str(max_retries)
    }

    def setUp(self):
        self.initial_execution_arn = 'arn:aws:states:us-south-10:98877654311:blah:a17h83j-p84321'
        self.subsequent_execution_arn = 'arn:aws:states:us-south-10:98877654311:blah:ab423cf-7753ae'
        self.terminal_fail_execution_arn = 'arn:aws:states:us-south-10:98877654311:blah:i3m556d-b5903fe'
        self.json_file = 'body_getTSData_3408_7664109d-4bf5-42eb-bb84-9505cd79137f.json'
        self.s3_bucket = 'iow-retriever-capture-dev'
        self.s3_url = f'https://s3.console.aws.amazon.com/s3/object/{self.s3_bucket}?region={self.region}&prefix={self.json_file}'
        self.s3_url_not_generated = 'json file could not be parsed from state machine input, no s3 url generated'
        self.cause = 'a blurb explaining why the step function execution failed'

        self.state_machine_start_input = {
            'Record': {'eventVersion': '2.1', 'eventSource': 'aws:s3'}
        }
        self.subsequent_start_input = {
            'Record': {'eventVersion': '2.1', 'eventSource': 'aws:s3'},
            'stepFunctionFails': 1,
            'previousExecutions': [self.initial_execution_arn]
        }
        self.terminal_fail_start_input = {
            'Record': {'eventVersion': '2.1', 'eventSource': 'aws:s3'},
            'previousExecutions': [self.initial_execution_arn, self.subsequent_execution_arn],
            'stepFunctionFails': 6
        }

        self.terminal_fail_start_input_with_json_file = {
            'Record': {
                'eventVersion': '2.1',
                'eventSource': 'aws:s3',
                's3': {
                    'bucket': {
                        'name': self.s3_bucket
                    },
                    'object': {
                        'key': self.json_file
                    }
                }
            },
            'previousExecutions': [self.initial_execution_arn, self.subsequent_execution_arn],
            'stepFunctionFails': 6
        }

        self.initial_event = {
            'executionArn': self.initial_execution_arn,
            'startInput': self.state_machine_start_input,
            'cause': self.cause
        }
        self.subsequent_event = {
            'executionArn': self.subsequent_execution_arn,
            'startInput': self.subsequent_start_input,
            'cause': self.cause
        }
        self.terminal_fail_event = {
            'executionArn': self.terminal_fail_execution_arn,
            'startInput': self.terminal_fail_start_input,
            'cause': self.cause
        }

        self.terminal_fail_event_with_json_file = {
            'executionArn': self.terminal_fail_execution_arn,
            'startInput': self.terminal_fail_start_input_with_json_file,
            'cause': self.cause
        }

        self.context = {'element': 'lithium'}

    @mock.patch.dict('persist_error.handler.os.environ', mock_env_vars)
    @mock.patch('persist_error.handler.send_message', autospec=True)
    def test_retry_message_sent(self, mock_sm):
        lambda_handler(self.initial_event, self.context)
        expected_message_body = {
            'Record': {'eventVersion': '2.1', 'eventSource': 'aws:s3'},
            'stepFunctionFails': 1,
            'previousExecutions': [self.initial_execution_arn]
        }
        mock_sm.assert_called_with(
            queue_url=self.queue_url,
            message_body=json.dumps(expected_message_body),
            region=self.region
        )

    @mock.patch.dict('persist_error.handler.os.environ', mock_env_vars)
    @mock.patch('persist_error.handler.send_message', autospec=True)
    def test_subsequent_retry_increment(self, mock_sm):
        lambda_handler(self.subsequent_event, self.context)
        expected_message_body = {
            'Record': {'eventVersion': '2.1', 'eventSource': 'aws:s3'},
            'stepFunctionFails': 2,
            'previousExecutions': [self.initial_execution_arn, self.subsequent_execution_arn]
        }
        mock_sm.assert_called_with(
            queue_url=self.queue_url,
            message_body=json.dumps(expected_message_body),
            region=self.region
        )

    @mock.patch.dict('persist_error.handler.os.environ', mock_env_vars)
    @mock.patch('persist_error.handler.send_message', autospec=True)
    @mock.patch('persist_error.handler.send_notification', autospec=True)
    def test_terminal_failure_behavior(self, mock_sn, mock_sm):
        lambda_handler(self.terminal_fail_event, self.context)
        expected_output = {
            'Record': {'eventVersion': '2.1', 'eventSource': 'aws:s3'},
            'previousExecutions': [
                self.initial_execution_arn,
                self.subsequent_execution_arn,
                self.terminal_fail_execution_arn
            ],
            'stepFunctionFails': 7
        }
        expected_notification_message_body = (
            f'Step function execution {self.terminal_fail_execution_arn} has terminally failed. \n'
            f'The file we attempted to process: {self.s3_url_not_generated} \n'
            f'This input has exceeded {self.max_retries} failures:\n'
            f'{json.dumps(expected_output, indent=4)}.\n'
            f'The execution reported this as the cause of the failure:\n'
            f'{self.cause}.\n'
            f'Please take a closer look at the underlying records and data.'
        )
        mock_sm.assert_not_called()
        mock_sn.assert_called_with(
            self.sns_arn,
            expected_notification_message_body,
            subject_line='Excessive Capture Failures Reported'
        )

    @mock.patch.dict('persist_error.handler.os.environ', mock_env_vars)
    @mock.patch('persist_error.handler.send_message', autospec=True)
    @mock.patch('persist_error.handler.send_notification', autospec=True)
    def test_terminal_failure_behavior_with_json_file(self, mock_sn, mock_sm):
        with warnings.catch_warnings(record=True) as w:
            lambda_handler(self.terminal_fail_event_with_json_file, self.context)
            # test warning behavior
            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[-1].category, UserWarning))
            # test AWS service integration behavior
            expected_output = {
                'Record': {
                    'eventVersion': '2.1',
                    'eventSource': 'aws:s3',
                    's3': {
                        'bucket': {
                            'name': self.s3_bucket
                        },
                        'object': {
                            'key': self.json_file
                        }
                    }
                },
                'previousExecutions': [
                    self.initial_execution_arn,
                    self.subsequent_execution_arn,
                    self.terminal_fail_execution_arn
                ],
                'stepFunctionFails': 7
            }
            expected_notification_message_body = (
                f'Step function execution {self.terminal_fail_execution_arn} has terminally failed. \n'
                f'The file we attempted to process: {self.s3_url} \n'
                f'This input has exceeded {self.max_retries} failures:\n'
                f'{json.dumps(expected_output, indent=4)}.\n'
                f'The execution reported this as the cause of the failure:\n'
                f'{self.cause}.\n'
                f'Please take a closer look at the underlying records and data.'
            )
            mock_sm.assert_not_called()
            mock_sn.assert_called_with(
                self.sns_arn,
                expected_notification_message_body,
                subject_line='Excessive Capture Failures Reported'
            )
