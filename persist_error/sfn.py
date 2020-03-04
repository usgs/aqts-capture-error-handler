"""
Module for working with the AWS Step Functions API.

"""
import json

import boto3

from .utils import search_dictionary_list


ENTRY_FAIL_MAPPING = {
    'MapStateFailed': 'MapStateEntered',
    'LambdaFunctionFailed': 'TaskStateEntered'
}


def get_execution_history(execution_arn, region='us-west-2'):
    """
    Get the full execution history of a Step Function run.
    The function will loop until all events have been
    retrieved.

    :param str execution_arn: execution of ARN of a particular Step Function run
    :param str region: AWS region, defaults to us-west-2
    :return: events in a step function run
    :rtype: dict

    """
    next_token = ''
    iter_count = 0
    events = []
    sfn = boto3.client('stepfunctions', region_name=region)
    while next_token or iter_count == 0:
        if next_token:
            history = sfn.get_execution_history(executionArn=execution_arn, nextToken=next_token)
        else:
            history = sfn.get_execution_history(executionArn=execution_arn)
        events.extend(history['events'])
        next_token = history.get('nextToken', '')
        iter_count += 1
    return {'events': events}


def backtrack_to_failure(execution_history):
    """
    From the most recent item in events, look
    for the most recent failure event.

    :param dict execution_history: dictionary containing the `events` key with a list value
    :return: the most recent failure event
    :rtype: dict

    """
    event_ids = [event['id'] for event in execution_history['events']]
    max_id = max(event_ids)  # find the event with highest ID to get the most recent
    event = search_dictionary_list(execution_history['events'], 'id', max_id)[0]
    task_type = event['type']
    while 'Failed' not in task_type:
        previous_event_id = event['previousEventId']
        search_result = search_dictionary_list(execution_history['events'], 'id', previous_event_id)[0]
        task_type = search_result['type']
        event = search_result
    return event


def find_root_failure_state(execution_history):
    """
    Get the original input that caused a state to fail
    and the name of the state.

    :param dict execution_history: dictionary containing the `events` key with a list value
    :return: original input with an extra `resumeState` key for the state name
    :rtype: dict

    """
    execution_events = execution_history['events']
    event = backtrack_to_failure(execution_history)
    failed_event_type = event['type']
    while event['type'] != ENTRY_FAIL_MAPPING[failed_event_type]:
        state_previous_event_id = event['previousEventId']
        search_result = search_dictionary_list(execution_events, 'id', state_previous_event_id)[0]
        event = search_result
    state_event_details = event['stateEnteredEventDetails']
    root_input = json.loads(state_event_details['input'])
    root_input['resumeState'] = state_event_details['name']
    return root_input
