"""
Tests for utility functions.

"""
from unittest import TestCase

from ..utils import search_dictionary_list


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
