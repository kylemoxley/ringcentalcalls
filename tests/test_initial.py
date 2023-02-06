#!/usr/bin/env python
"""Tests for `current` package."""
# pylint: disable=redefined-outer-name
import os
import sys
from pathlib import Path
import pytest

sys.path.insert(0, 'ringcentalcalls/ringcentalcalls')

### transform time to minutes tests
test_dirs = [([
        'data',
        'data/bronze',
        'data/silver',
        'data/gold',
        'docs',
        'models',
        'pipelines',
        'sql'
    ], True)]
@pytest.mark.parametrize('input, expected', test_dirs)
def test_folders(input, expected):

    expected_dirs = input

    ignored_dirs = []
    path = Path().resolve()
    parent_path = path

    abs_expected_dirs = [str(parent_path / d) for d in expected_dirs]

    all_folders_exist = True
    failed_folder = ""
    for current_folder_path in abs_expected_dirs:
        print(f"iter: {current_folder_path}", os.path.isdir(current_folder_path))
        if not os.path.isdir(current_folder_path):
            failed_folder = current_folder_path
            all_folders_exist = False
            break
    assert all_folders_exist == expected, f"The folder {failed_folder} does not exists."


test_files_var = [([
        'README.md',
        'LICENSE',
        'requirements.txt',
    ], 0)]
@pytest.mark.parametrize('input, expected', test_files_var)
def test_files(input, expected):
    path = Path().resolve()
    missing_files_list = []
    for current_expected_file in input:
        if not os.path.isfile(str(path / current_expected_file)):
            missing_files_list.append(current_expected_file)

    assert len(missing_files_list) == expected, f"They are missing the following files: {missing_files_list}"

path_to_utils_module = Path('ringcentalcalls/ringcentalcalls/utils.py')
if path_to_utils_module.is_file():

    from utils import month_diff

    test_dates = [('2022-11-03', 4, '2022-07-03'), ('2010-06-14', 4, '2010-02-14')]
    @pytest.mark.parametrize('input_date, number_of_months, expected', test_dates)
    def test_month_diff(input_date, number_of_months, expected):
        result = month_diff(input_date, number_of_months)
        assert result == expected, f"the date {result} does not match with the expected date {expected}."

path_to_preprocess_module = Path('ringcentalcalls/ringcentalcalls/preprocess.py')
if path_to_preprocess_module.is_file():
    from preprocess import clean_columns
    import pandas as pd

    # initialize list elements
    data = [[10,20]]
    test_columns_df = pd.DataFrame(data, columns=['field1 -__&$', "field2></'___"])
    expected_columns_df = ['Field1__', 'Field2__']

    test_dfs = [(test_columns_df, expected_columns_df), ]
    @pytest.mark.parametrize('input_test_columns_df, expected_column_names', test_dfs)
    def test_clean_columns(input_test_columns_df, expected_column_names):
        result = list(clean_columns(input_test_columns_df).columns)
        print(result, expected_column_names)
        assert result == expected_column_names, f"the column names {result} do not match with the expected column names {expected_column_names}."
