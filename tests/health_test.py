import requests
import csv
import pytest


@pytest.fixture
def url_list():
    """
    List of preivously broken URLs to check to insure no regressions
    """
    with open("tests/url_list.csv") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",")
        all_urls = []
        for row in csv_reader:
            all_urls.extend(row)
        # Remove empty strings from hanging commas
        all_urls = list(filter(None, all_urls))
        return all_urls


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
