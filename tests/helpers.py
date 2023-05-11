# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import csv
import json
import os

import pytest


def compare_lines_ignore_order(expected, actual):
    expected_lines = expected.splitlines()
    actual_lines = actual.splitlines()

    actual_records = None
    expected_records = None

    # try parse lines as ndjson
    try:
        actual_records = [json.loads(line) for line in actual_lines]
        expected_records = [json.loads(line) for line in expected_lines]
    except json.decoder.JSONDecodeError:
        pass

    if actual_records is None:
        # otherwise try parse lines as csv
        try:
            actual_records = list(csv.DictReader(actual_lines))
            expected_records = list(csv.DictReader(expected_lines))
        except csv.Error:
            pass

    if actual_records is None or expected_records is None:
        expected_lines.sort()
        actual_lines.sort()

        assert actual_lines == expected_lines
    else:
        actual_records.sort(key=lambda x: json.dumps(x, sort_keys=True))
        expected_records.sort(key=lambda x: json.dumps(x, sort_keys=True))

        assert actual_records == expected_records

        # assert json.dumps(actual_records, sort_keys=True, indent=4) == json.dumps(
        #     expected_records, sort_keys=True, indent=4
        # )


def read_file(path):
    if not os.path.exists(path):
        return ''
    with open(path) as file:
        return file.read()


run_slow_tests_variable = os.environ.get('ETHEREUM_ETL_RUN_SLOW_TESTS', 'False')
run_slow_tests = run_slow_tests_variable.lower() in ['1', 'true', 'yes']


def skip_if_slow_tests_disabled(data):
    return pytest.param(
        *data, marks=pytest.mark.skipif(not run_slow_tests, reason='Skipping slow running tests')
    )
