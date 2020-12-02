import requests
import pytest


@pytest.fixture
def url_list():
    """
    List of preivously broken URLs to check to insure no regressions
    """
    return [
        "https://ytr9800fbk.execute-api.us-east-1.amazonaws.com/averages?temporal=dow&parameter=pm10&location=2&spatial=location",
        "https://ytr9800fbk.execute-api.us-east-1.amazonaws.com/measurements?location=%E4%BA%91%E6%A0%96&page=1&limit=10000&date_from=2020-11-19T18%3A00%3A00.000Z&date_to=2020-11-27T18%3A00%3A00.000Z",
        "https://ytr9800fbk.execute-api.us-east-1.amazonaws.com/measurements?location=%E9%9D%92%E6%B3%A5%E6%B4%BC%E6%A1%A5&page=1&limit=10000&date_from=2020-11-19T18%3A00%3A00.000Z&date_to=2020-11-27T18%3A00%3A00.000Z",
        "https://ytr9800fbk.execute-api.us-east-1.amazonaws.com/averages?temporal=moy&parameter=pm10&spatial=location&location=9",
        "https://ytr9800fbk.execute-api.us-east-1.amazonaws.com/measurements",
        "https://ytr9800fbk.execute-api.us-east-1.amazonaws.com/projects",
        "https://ytr9800fbk.execute-api.us-east-1.amazonaws.com/countries",
        "https://ytr9800fbk.execute-api.us-east-1.amazonaws.com/locations",
    ]


@pytest.fixture
def max_wait():
    """
    The maximum amount of time we want to allow highly-used requests to run for before we question of there is an index or other type of error
    """
    return 4


def test_ok_status(url_list, max_wait):
    """
    Assert 1 - Confirm that frequently used URLs return OK status codes
    Assert 2 - Confirm that frequently used URLs respond within our desired time window
    """
    for url in url_list:
        r = requests.get(url)
        assert r.status_code == requests.codes.ok
        assert r.elapsed.total_seconds() < max_wait


# def test_speed(url_list, max_wait):
#     """
#     Confirm that frequently used URLs respond within our desired time window
#     """
#     for url in url_list:
#         r = requests.get(url)
#         assert r.elapsed.total_seconds() < max_wait
