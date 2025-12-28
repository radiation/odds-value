from typing import Any


def test_odds_api_response_type_guard():
    data: Any = {"foo": "bar"}
    assert isinstance(data, dict | list)
