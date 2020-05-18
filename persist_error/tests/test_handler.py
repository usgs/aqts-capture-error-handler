"""
Tests for the AWS Lambda handler.

"""
from datetime import datetime
import json
from unittest import TestCase, mock

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

        self.initial_event = {'executionArn': self.initial_execution_arn, 'startInput': self.state_machine_start_input}
        self.subsequent_event = {
            'executionArn': self.subsequent_execution_arn,
            'startInput': self.subsequent_start_input
        }
        self.terminal_fail_event = {
            'executionArn': self.terminal_fail_execution_arn,
            'startInput': self.terminal_fail_start_input
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
            f'Step function execution {self.terminal_fail_execution_arn} has terminally failed. '
            f'This input has exceeded {self.max_retries} failures: {expected_output}.\n'
            f'Please take a closer look at the underlying records and data.'
        )
        mock_sm.assert_not_called()
        mock_sn.assert_called_with(
            self.sns_arn,
            expected_notification_message_body,
            subject_line='Excessive Capture Failures Reported'
        )
