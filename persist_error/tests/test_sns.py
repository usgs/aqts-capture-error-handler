"""
Tests for SNS functions.

"""
from unittest import TestCase, mock

from ..sns import send_notification


class TestSendNotification(TestCase):

    def setUp(self):
        self.sns_arn = 'arn:aws:sns:us-south-19:5746521541:fake-notification'
        self.region = 'us-south-19'
        self.payload = 'fake payload'

    @mock.patch('persist_error.sns.boto3.client', autospec=True)
    def test_send_notification(self, m_client):
        m_sns = mock.Mock()
        m_client.return_value = m_sns

        send_notification(self.sns_arn, self.payload, self.region)
        m_client.assert_called_with('sns', region_name=self.region)
        m_sns.publish.assert_called_with(
            TopicArn=self.sns_arn,
            Message=self.payload,
            Subject=f'Excessive Capture Failures Reported'
        )
