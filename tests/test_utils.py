from copy import deepcopy

import pytest

from kedro_azureml.utils import update_dict


@pytest.mark.parametrize(
    "input_dict, kv_pairs, expected_output",
    [
        ({}, [("a", 1)], {"a": 1}),
        ({"a": 1}, [("a", 2)], {"a": 2}),
        ({"a": {"b": 1}}, [("a.b", 2)], {"a": {"b": 2}}),
        ({"a": {"b": {"c": 1}}}, [("a.b.c", 2)], {"a": {"b": {"c": 2}}}),
        (
            {"a": {"b": {"c": 1}}},
            [("a.b.c", 2), ("a.b.d", 3)],
            {"a": {"b": {"c": 2, "d": 3}}},
        ),
        ({}, [("a.b.c", 1)], {"a": {"b": {"c": 1}}}),
    ],
)
def test_update_dict(input_dict, kv_pairs, expected_output):
    copied_dict = deepcopy(input_dict)
    actual_output = update_dict(input_dict, *kv_pairs)
    assert actual_output == expected_output, "update is incorrect"
    assert actual_output is not input_dict, "output should be a deep copy"
    assert input_dict == copied_dict, "input_dict should not be mutated"
