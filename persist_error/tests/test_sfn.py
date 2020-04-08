"""
Module for testing functions interacting with the step functions API.

"""
from datetime import datetime
from unittest import TestCase, mock

from ..sfn import get_execution_history, backtrack_to_failure, find_root_failure_state


class TestGetExecutionHistory(TestCase):

    def setUp(self):
        self.execution_arn = 'arn:aws:states:us-south-10:98877654311:blah:873c-33v32x'
        self.region = 'us-south-10'
        self.resp_0 = {
            'nextToken': 'nextTokenString',
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
                }
            ]
        }
        self.resp_1 = {
            'events': [
                {
                    'timestamp': datetime(2375, 5, 6, 17, 21, 17),
                    'id': 3,
                    'previousEventId': 2,
                    'type': 'LambdaFunctionFailed',
                    'lambdaFunctionFailedEventDetails': {
                        'cause': '{"errorMessage": "ValueError"}'
                    }
                }
            ]
        }

    @mock.patch('persist_error.sfn.boto3.client', autospec=True)
    def test_single_page_resp(self, m_client):
        mock_sfn = mock.Mock()
        mock_sfn.get_execution_history.return_value = self.resp_1
        m_client.return_value = mock_sfn

        resp = get_execution_history(self.execution_arn, self.region)
        m_client.assert_called_with('stepfunctions', region_name=self.region)
        mock_sfn.get_execution_history.assert_called_with(
            executionArn=self.execution_arn
        )
        self.assertEqual(len(resp['events']), 1)

    @mock.patch('persist_error.sfn.boto3.client', autospec=True)
    def test_multiple_page_resp(self, m_client):
        mock_sfn = mock.Mock()
        mock_sfn.get_execution_history.side_effect = [self.resp_0, self.resp_1]
        m_client.return_value = mock_sfn

        resp = get_execution_history(self.execution_arn, self.region)
        m_client.assert_called_once()
        mock_sfn.get_execution_history.assert_called()
        self.assertEqual(len(resp['events']), 3)


class TestBacktrackToFailure(TestCase):

    def setUp(self):
        self.execution_history_failure = {
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
        self.execution_history_timeout = {
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
                    'lambdaFunctionTimedOutEventDetails': {
                        'cause': '{"error": "States.Timeout"}'
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

    def test_finding_failed_state(self):
        result = backtrack_to_failure(self.execution_history_failure)
        self.assertDictEqual(
            result, self.execution_history_failure['events'][2]
        )

    def test_finding_timeout_state(self):
        result = backtrack_to_failure(self.execution_history_timeout)
        self.assertDictEqual(
            result, self.execution_history_timeout['events'][2]
        )


class TestFindRootFailure(TestCase):

    def setUp(self):
        self.execution_history = {
            'events': [
                {
                    'timestamp': datetime(2375, 5, 6, 17, 20, 33),
                    'id': 1,
                    'previousEventId': 0,
                    'type': 'ExecutionStated',
                    'executionStartedEventDetails': {'input': '{"value": 3}'}
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

    def test_find_root_failure_input(self):
        result = find_root_failure_state(self.execution_history)
        self.assertDictEqual(result, {'value': '3', 'resumeState': 'someState'})
