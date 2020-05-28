"""
Tests for utility functions.

"""
from unittest import TestCase, mock

from ..utils import search_dictionary_list, select_delay_seconds


class TestSearchDictionaryList(TestCase):

    def setUp(self):
        self.dict_list = [
            {'id': 0, 'value': 'mango'},
            {'id': 1, 'value': 'apple'},
            {'id': 2, 'value': 'cherry'},
            {'id': 3, 'value': 'mango'}
        ]

    def test_single_match(self):
        result = search_dictionary_list(self.dict_list, 'id', 0)
        self.assertListEqual(result, self.dict_list[0:1])

    def test_multiple_match(self):
        result = search_dictionary_list(self.dict_list, 'value', 'mango')
        self.assertListEqual(result, [self.dict_list[0], self.dict_list[-1]])


class TestSelectDelaySeconds(TestCase):

    def setUp(self):
        self.low = 20
        self.high = 40

    @mock.patch('persist_error.utils.np.random', autospec=True)
    def test_delay_seconds_np_call(self, mock_np_rd):
        select_delay_seconds(low=self.low, high=self.high)
        mock_np_rd.randint.assert_called_with(self.low, self.high+1)
