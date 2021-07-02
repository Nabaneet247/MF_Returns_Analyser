from datetime import datetime

import numpy as np
import pandas as pd
from loguru import logger

import file_locations
from data_frame_utils import convert_data_frame_to_list_of_chunks_row_wise
from date_utils import re_format_date, get_valid_dates_for_a_year
from file_types import get_file_type_label


def read_latest_data(file_type):
    current_year = datetime.now().year
    data_for_current_year = get_data_for_a_year(file_type, str(current_year))
    if len(data_for_current_year.columns.values) < 35:
        data_for_previous_year = get_data_for_a_year(file_type, str(current_year - 1))
        data = data_for_previous_year.join(data_for_current_year.set_index(['Fund House Name', 'Scheme Name']),
                                           on=['Fund House Name', 'Scheme Name'])
    else:
        data = data_for_current_year
    return data


def get_list_of_all_valid_years():
    current_year = datetime.now().year
    return [str(x) for x in range(2006, current_year + 1)]


def get_combined_year_wise_data_chunks(file_type, chunks_count=10):
    data = get_combined_year_wise_data(file_type)
    return convert_data_frame_to_list_of_chunks_row_wise(data, chunks_count)


def get_data_for_a_year_as_chunks(file_type, year, chunks_count=10):
    data = get_data_for_a_year(file_type, year)
    return convert_data_frame_to_list_of_chunks_row_wise(data, chunks_count)


def get_combined_year_wise_data(file_type, print_info_logs=True):
    years = get_list_of_all_valid_years()
    data = get_list_of_all_schemes()
    for year in years:
        try:
            year_wise_data = get_data_for_a_year(file_type, year)
            if print_info_logs:
                logger.info('{} dates, {} schemes are present for {}', len(year_wise_data.columns.values) - 2,
                            len(year_wise_data.index), year)
            data = data.join(year_wise_data.set_index(['Fund House Name', 'Scheme Name']),
                             on=['Fund House Name', 'Scheme Name'])
        except FileNotFoundError:
            logger.error('{} file not found for {}', get_file_type_label(file_type), year)
    return data


def get_data_for_a_year(file_type, year):
    dates = get_valid_dates_for_a_year(year)
    float_cols = {col: np.float32 for col in dates}
    return read_csv_file(get_file_name_from_year_and_type(file_type, year), dtypes=float_cols)


def get_analysed_file_for_date(date):
    date_format = '%Y-%m-%d'
    formatted_date_string = pd.to_datetime(date, format='%d-%m-%Y').strftime(date_format)
    file_name = file_locations.ANALYSED_DATA_FOLDER_PATH + '{} Analysis'.format(formatted_date_string)
    return read_csv_file(file_name)


def read_csv_file(file_name, dtypes=None):
    file_name += '.csv'
    if dtypes is None:
        return pd.read_csv(file_name)
    else:
        return pd.read_csv(file_name, dtype=dtypes)


def get_test_nav_data():
    return get_combined_year_wise_data('test nav')


def get_org_nav_data():
    return get_combined_year_wise_data('org nav')


def get_cleaned_nav_data():
    return get_combined_year_wise_data('cln nav')


def get_file_name_from_year_and_type(file_type, year):
    label = get_file_type_label(file_type)
    return file_locations.PROCESSED_DATA_FOLDER_PATH + label + '_' + year


def get_list_of_all_schemes():
    return read_csv_file(file_locations.SCHEMES_LIST_FILE_PATH)


def read_html_nav_data(date):
    reformatted_date = re_format_date(date, '%d-%m-%Y', '%Y-%m-%d')
    file_name = '{}_{}.html'.format(file_locations.AMFI_HTML_DATA_FILE_PATH, reformatted_date)
    try:
        file_reader = open(file_name, 'r')
        html_data = file_reader.read()
        return html_data
    except Exception as e:  # todo
        logger.error('Faced error while reading html data for date {} \n {} - {}\n', date, type(e), e)
    return ''
