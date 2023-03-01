# -*- coding: utf-8 -*-

"""Project-wide utils."""

import json
from urllib.request import urlopen

import pycountry

from src.exceptions import UnsupportedValueError


TIMEOUT = 30


def clean_url(url):
    # Clean URL.
    if url.startswith("<"):
        url = url[1:]
    if url.endswith(">"):
        url = url[:-1]
    return url


def _read_github_recursive(url, timeout, collected):
    """
    Recoursively read url until pagintion is exhausted.

    :param collected: <list> - elements collected so far.
    """
    resp = urlopen(url, timeout=timeout)
    info = resp.info()
    content_type = info.get_content_type()
    if 'application/json' not in content_type:
        raise UnsupportedValueError(
            "Only JSON responses are supported (request returned `{}`)"\
            .format(content_type)
            )

    charset = info.get_content_charset()
    raw = resp.read().decode(charset)
    serialized = json.loads(raw)
    if isinstance(serialized, list):
        collected.extend(serialized)
    else:
        collected.append(serialized)

    # Pagination.
    headers = resp.getheaders()
    headers = dict(h for h in headers)
    try:
        pages = [x.strip() for x in headers['Link'].split(",")]
    except (AttributeError, KeyError):
        return collected

    for page in pages:
        url, rel = [p.strip() for p in page.split(";")]
        if rel == 'rel="next"':
            url = clean_url(url)
            collected = _read_github_recursive(url, timeout, collected)

    return collected


def read_github(url, timeout=TIMEOUT):
    """
    :param url: <str> - only the meaningful part of the URL
        (e.g. 'users/whosonfirst-data').
    :param timeout: <int>
    """
    collected = []
    return _read_github_recursive(url, timeout, collected)


def format_error(err):
    return "%s (%s)" % (err, type(err).__name__)


def country_name(country_code):
    """
    Return country name by country code.

    :param country_code: <str>
    :return: <str>
    """
    country_code = country_code.strip()
    code_length = len(country_code)
    if code_length > 2:
        raise UnsupportedValueError("`country_code` can only be 2 or 3 symbols long!")

    alpha_name = "alpha_{}".format(code_length)
    country = pycountry.countries.get(**{alpha_name: country_code})

    return country.name
