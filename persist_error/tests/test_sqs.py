"""
Tests for the sqs module

"""
from unittest import TestCase, mock

from ..sqs import send_message


class TestSendMessage(TestCase):

    def setUp(self):
        self.queue_url = 'https://sqs.us-south-10.amazonaws.com/887501/some-queue-name'
        self.message_body = 'a message body'
        self.region = 'us-south-10'

    @mock.patch('persist_error.sqs.boto3.client', autospec=True)
    def test_message_send(self, m_client):
        mock_sqs = mock.Mock()
        m_client.return_value = mock_sqs

        send_message(
            queue_url=self.queue_url,
            message_body=self.message_body,
            region=self.region
        )
        m_client.assert_called_with('sqs', region_name=self.region)
        mock_sqs.send_message.assert_called_with(
            QueueUrl=self.queue_url,
            MessageBody=self.message_body
        )
