"""
Tests for the sqs module

"""
from unittest import TestCase, mock

from ..sqs import send_message


class TestSendMessage(TestCase):

    def setUp(self):
        self.queue_url = 'https://sqs.us-south-10.amazonaws.com/887501/some-queue-name'
        self.message_body = 'a message body'
        self.message_grp_id = 'message_id'
        self.deduplication_id = '584-2ab-3381z'
        self.region = 'us-south-10'

    @mock.patch('persist_error.sqs.uuid')
    @mock.patch('persist_error.sqs.boto3.client', autospec=True)
    def test_message_send(self, m_client, m_uuid):
        mock_sqs = mock.Mock()
        m_client.return_value = mock_sqs

        m_uuid.uuid4.return_value = self.deduplication_id

        send_message(
            queue_url=self.queue_url,
            message_body=self.message_body,
            message_grp_id=self.message_grp_id,
            region=self.region
        )
        m_client.assert_called_with('sqs', region_name=self.region)
        mock_sqs.send_message.assert_called_with(
            QueueUrl=self.queue_url,
            MessageBody=self.message_body,
            MessageDeduplicationId=self.deduplication_id,
            MessageGroupId=self.message_grp_id
        )
