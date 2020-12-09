import requests
import pytest
import os
import schemathesis
from hypothesis import settings


BASEURL=os.getenv('OPENAQ_FASTAPI_URL')

schemathesis.fixups.install()
schema = schemathesis.from_uri(f"{BASEURL}/openapi.json")


@pytest.fixture
def url_list():
    """
    List of preivously broken URLs to check to insure no regressions
    """

    with open("./url_list.txt") as file:
        urls = [BASEURL + line.rstrip() for line in file]
    return urls


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
        print(url)
        r = requests.get(url)
        assert r.status_code == requests.codes.ok
        assert r.elapsed.total_seconds() < max_wait


@schema.parametrize()
@settings(max_examples=50, deadline=15000)
def test_api(case):
    case.call_and_validate()
