"""
Utility functions

"""


def search_dictionary_list(dict_list, search_key, search_value):
    result = list(filter(lambda x: x[search_key] == search_value, dict_list))
    sorted_result = sorted(result, key=lambda y: y['timestamp'])
    return sorted_result
