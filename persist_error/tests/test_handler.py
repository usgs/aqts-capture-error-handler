"""
Tests for the AWS Lambda handler

"""
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
        self.event = {'spam': 'eggs'}
        self.context = {'element': 'lithium'}

    @mock.patch.dict('persist_error.handler.os.environ', mock_env_vars)
    @mock.patch('persist_error.handler.send_message')
    def test_event_handling(self, mock_sm):
        result = lambda_handler(self.event, self.context)
        self.assertEqual(result, self.event)
        mock_sm.assert_called_with(
            queue_url=self.queue_url,
            message_body=json.dumps(self.event),
            region=self.region
        )
