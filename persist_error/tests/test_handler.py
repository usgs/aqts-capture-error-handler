"""
Tests for the AWS Lambda handler

"""
from datetime import datetime
import json
from unittest import TestCase, mock

from ..handler import lambda_handler


class TestLambdaHandler(TestCase):

    queue_url = 'https://sqs.us-south-10.amazonaws.com/887501/some-queue-name'
    region = 'us-south-10'
    mock_env_vars = {
        'AWS_SQS_QUEUE_URL': queue_url,
        'AWS_DEPLOYMENT_REGION': region
    }

    def setUp(self):
        self.execution_arn = 'arn:aws:states:us-south-10:98877654311:blah:a17h83j-p84321'
        self.event = {'executionArn': self.execution_arn}
        self.context = {'element': 'lithium'}
        self.execution_history = {
            'events': [
                {
                    'timestamp': datetime(2375, 5, 6, 17, 20, 33),
                    'id': 1,
                    'previousEventId': 0,
                    'type': 'ExecutionStated',
                    'executionStatedEventDetails': {'input': '{"value": 3}'}
                },
                {
                    'timestamp': datetime(2375, 5, 6, 17, 21, 5),
                    'id': 2,
                    'previousEventId': 1,
                    'type': 'TaskStateEntered',
                    'stateEnteredEventDetails': {
                        'name': 'someState',
                        'input': '{"value": "3"}'
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

    @mock.patch.dict('persist_error.handler.os.environ', mock_env_vars)
    @mock.patch('persist_error.handler.send_message')
    @mock.patch('persist_error.handler.get_execution_history')
    def test_event_handling(self, mock_eh, mock_sm):
        mock_eh.return_value = self.execution_history
        result = lambda_handler(self.event, self.context)

        mock_eh.assert_called_once()
        expected_result = {
            'value': '3',
            'resumeState': 'someState',
            'stepFunctionFails': 1
        }
        self.assertDictEqual(result, expected_result)
        mock_sm.assert_called_with(
            queue_url=self.queue_url,
            message_body=json.dumps(expected_result),
            region=self.region
        )
