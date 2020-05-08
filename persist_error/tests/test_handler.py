"""
Tests for the AWS Lambda handler.

"""
from datetime import datetime
import json
from unittest import TestCase, mock

from botocore.exceptions import ClientError

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
        self.fail_execution_arn = 'arn:aws:states:us-south-10:98877654311:blah:i3m556d-b5903fe'
        self.state_machine_start_input = {'Record': {'eventVersion': '2.1', 'eventSource': 'aws:s3'}}
        self.excessive_start_input = {
            'Record': {'value': '3'},
            'resumeState': 'someState',
            'previousExecutions': [{self.initial_execution_arn}],
            'stepFunctionFails': 6
        }
        self.initial_event = {'executionArn': self.initial_execution_arn, 'startInput': self.state_machine_start_input}
        self.excessive_fail_event = {
            'executionArn': self.fail_execution_arn,
            'startInput': self.excessive_start_input
        }
        self.context = {'element': 'lithium'}
        self.initial_execution_history = {
            'events': [
                {
                    'timestamp': datetime(2379, 5, 6, 17, 20, 33),
                    'id': 1,
                    'previousEventId': 0,
                    'type': 'ExecutionStated',
                    'executionStartedEventDetails': {'input': '{"value": 3}'}
                },
                {
                    'timestamp': datetime(2379, 5, 6, 17, 21, 5),
                    'id': 2,
                    'previousEventId': 1,
                    'type': 'TaskStateEntered',
                    'stateEnteredEventDetails': {
                        'name': 'someState',
                        'input': '{"value": "3"}'
                    }
                },
                {
                    'timestamp': datetime(2379, 5, 6, 17, 21, 17),
                    'id': 3,
                    'previousEventId': 2,
                    'type': 'LambdaFunctionFailed',
                    'lambdaFunctionFailedEventDetails': {
                        'cause': '{"errorMessage": "ValueError"}'
                    }
                },
                {
                    'timestamp': datetime(2379, 5, 6, 17, 21, 17),
                    'id': 4,
                    'previousEventId': 3,
                    'type': 'TaskStateEntered',
                    'stateEnteredEventDetails': {
                        'name': 'someOtherState',
                        'input': '{"value": "3"}'
                    }
                }
            ]
        }
        self.excessive_fail_execution_history = {
            'events': [
                {
                    'timestamp': datetime(2375, 5, 6, 17, 20, 33),
                    'id': 1,
                    'previousEventId': 0,
                    'type': 'ExecutionStated',
                    'executionStartedEventDetails': {'input': '{"value": 3, "resumeState": "someState"}'}
                },
                {
                    'timestamp': datetime(2375, 5, 6, 17, 21, 5),
                    'id': 2,
                    'previousEventId': 1,
                    'type': 'TaskStateEntered',
                    'stateEnteredEventDetails': {
                        'name': 'someState',
                        'input': f'{{"value": 3, "stepFunctionFails": 6, "previousExecutions": ["{self.initial_execution_arn}"]}}'
                    }
                },
                {
                    'timestamp': datetime(2375, 5, 6, 17, 21, 17),
                    'id': 3,
                    'previousEventId': 2,
                    'type': 'LambdaFunctionFailed',
                    'lambdaFunctionFailedEventDetails': {
                        'cause': '{"errorMessage": "ValueError"}'
                    }
                },
                {
                    'timestamp': datetime(2375, 5, 6, 17, 21, 17),
                    'id': 4,
                    'previousEventId': 3,
                    'type': 'TaskStateEntered',
                    'stateEnteredEventDetails': {
                        'name': 'someOtherState',
                        'input': '{"value": "3"}'
                    }
                }
            ]
        }
        self.different_state_fail_execution_history = {
            'events': [
                {
                    'timestamp': datetime(2375, 5, 6, 17, 20, 33),
                    'id': 1,
                    'previousEventId': 0,
                    'type': 'ExecutionStated',
                    'executionStartedEventDetails': {'input': '{"value": 3, "resumeState": "someState"}'}
                },
                {
                    'timestamp': datetime(2375, 5, 6, 17, 21, 5),
                    'id': 2,
                    'previousEventId': 1,
                    'type': 'TaskStateEntered',
                    'stateEnteredEventDetails': {
                        'name': 'someState',
                        'input': '{"value": 3, "stepFunctionFails": 6}'
                    }
                },
                {
                    'timestamp': datetime(2375, 5, 6, 17, 21, 17),
                    'id': 3,
                    'previousEventId': 2,
                    'type': 'LambdaFunctionSucceeded',
                    'stateExitedEventDetails': {
                        'output': '{"value": "17"}'
                    }
                },
                {
                    'timestamp': datetime(2375, 5, 6, 17, 21, 17),
                    'id': 4,
                    'previousEventId': 3,
                    'type': 'TaskStateEntered',
                    'stateEnteredEventDetails': {
                        'name': 'someOtherState',
                        'input': '{"value": "3"}'
                    }
                },
                {
                    'timestamp': datetime(2375, 5, 6, 17, 21, 17),
                    'id': 5,
                    'previousEventId': 4,
                    'type': 'LambdaFunctionFailed',
                    'lambdaFunctionFailedEventDetails': {
                        'cause': '{"errorMessage": "ValueError"}'
                    }
                },
                {
                    'timestamp': datetime(2375, 5, 6, 17, 21, 17),
                    'id': 6,
                    'previousEventId': 5,
                    'type': 'TaskStateEntered',
                    'stateEnteredEventDetails': {
                        'name': 'someOtherState',
                        'input': '{"value": "4"}'
                    }
                },
            ]
        }

    @mock.patch.dict('persist_error.handler.os.environ', mock_env_vars)
    @mock.patch('persist_error.handler.send_message', autospec=True)
    @mock.patch('persist_error.handler.get_execution_history', autospec=True)
    def test_basic_event_handling(self, mock_eh, mock_sm):
        mock_eh.return_value = self.initial_execution_history
        result = lambda_handler(self.initial_event, self.context)

        mock_eh.assert_called_once()
        expected_result = {
            'value': '3',
            'resumeState': 'someState',
            'stepFunctionFails': 1,
            'previousExecutions': [self.initial_execution_arn]
        }
        self.assertDictEqual(result, expected_result)
        mock_sm.assert_called_with(
            queue_url=self.queue_url,
            message_body=json.dumps(expected_result),
            region=self.region
        )

    @mock.patch.dict('persist_error.handler.os.environ', mock_env_vars)
    @mock.patch('persist_error.handler.send_message', autospec=True)
    @mock.patch('persist_error.handler.send_notification', autospec=True)
    @mock.patch('persist_error.handler.get_execution_history', autospec=True)
    def test_excessive_failures(self, mock_eh, mock_sn, mock_sm):
        mock_eh.return_value = self.excessive_fail_execution_history
        lambda_handler(self.excessive_fail_event, self.context)
        mock_eh.assert_called_once()
        mock_sn.assert_called_with(
            self.sns_arn,
            (f"Step function execution {self.fail_execution_arn} has terminally failed. "
             f"This input has exceeded {self.max_retries} failures for an individual state:"
             f" {{'value': 3, 'stepFunctionFails': {self.max_retries + 1}, 'previousExecutions': ['{self.initial_execution_arn}', '{self.fail_execution_arn}'], 'resumeState': 'someState'}}.\n"
             "Please take a closer look at the underlying records and data.")
        )
        mock_sm.assert_not_called()

    @mock.patch.dict('persist_error.handler.os.environ', mock_env_vars)
    @mock.patch('persist_error.handler.send_message', autospec=True)
    @mock.patch('persist_error.handler.send_notification', autospec=True)
    @mock.patch('persist_error.handler.get_execution_history', autospec=True)
    def test_failure_count_reset(self, mock_eh, mock_sn, mock_sm):
        mock_eh.return_value = self.different_state_fail_execution_history
        lambda_handler(self.excessive_fail_event, self.context)
        mock_eh.assert_called_once()
        mock_sn.assert_not_called()
        mock_sm.assert_called_once()

    @mock.patch.dict('persist_error.handler.os.environ', mock_env_vars)
    @mock.patch('persist_error.handler.send_message', autospec=True)
    @mock.patch('persist_error.handler.send_notification', autospec=True)
    @mock.patch('persist_error.handler.get_execution_history', autospec=True)
    @mock.patch('persist_error.handler.find_root_failure_state', autospec=True)
    def test_failure_trace_exception(self, mock_frfs, mock_eh, mock_sn, mock_sm):
        mock_frfs.side_effect = ValueError
        mock_eh.return_value = self.initial_execution_history
        lambda_handler(self.initial_event, self.context)
        mock_eh.assert_called_once()
        mock_sn.assert_called_with(
            self.sns_arn,
            (f'Human intervention required for execution {self.initial_execution_arn}. '
             'Unable to figure out what went wrong with this execution.')
        )
        mock_sm.assert_not_called()

    @mock.patch.dict('persist_error.handler.os.environ', mock_env_vars)
    @mock.patch('persist_error.handler.send_message', autospec=True)
    @mock.patch('persist_error.handler.get_execution_history', autospec=True)
    def test_api_client_error(self, mock_eh, mock_sm):
        mock_eh.side_effect = ClientError(
            error_response={'Error': {'Code': 'SomeCode'}},
            operation_name='MyOperation'
        )
        lambda_handler(self.initial_event, self.context)
        mock_eh.assert_called_once()
        mock_sm.assert_called_with(
            queue_url=self.queue_url,
            message_body=json.dumps(self.state_machine_start_input),
            region=self.region
        )

