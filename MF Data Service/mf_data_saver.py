from datetime import datetime

import pandas as pd
from loguru import logger

import file_locations
from Interval import Interval
from data_frame_utils import sort_columns_date_wise
from date_utils import re_format_date
from file_types import get_file_type_label
from mf_data_provider import get_list_of_all_schemes, get_data_for_a_year


def save_analysed_data(data, last_date_analysed=None):
    date_format = '%Y-%m-%d'
    if last_date_analysed is None:
        file_date_label = pd.to_datetime(datetime.now()).strftime(date_format)
    else:
        file_date_label = pd.to_datetime(last_date_analysed, format='%d-%m-%Y').strftime(date_format)
    file_name = file_locations.ANALYSED_DATA_FOLDER_PATH + '{} Analysis'.format(file_date_label)
    save_data_as_csv(data, file_name)
    logger.success('Analysed Data was saved at {}', file_name)


def save_html_plot(plot, date, interval: Interval, file_name_prefix=''):
    date_format = '%Y-%m-%d'
    formatted_date = pd.to_datetime(date, format='%d-%m-%Y').strftime(date_format)
    if file_name_prefix is not None and len(file_name_prefix) > 0:
        file_name = file_locations.PLOTS_FOLDER_PATH + '{} {} {} MF Returns Plot.html'.format(formatted_date,
                                                                                              interval.change_rate_label,
                                                                                              file_name_prefix)
    else:
        file_name = file_locations.PLOTS_FOLDER_PATH + '{} {} MF Returns Plot.html'.format(formatted_date,
                                                                                           interval.change_rate_label)
    plot.write_html(file_name, auto_open=False)
    logger.success('Plotted Data has been saved at {}', file_name)


def save_data_as_csv(data, file_name):
    file_name += '.csv'
    data.to_csv(file_name, index=False)
    logger.success('Data has been saved at {}', file_name)


def save_data_as_excel(data, file_name):
    file_name += '.xlsx'
    data.to_excel(file_name, index=False)
    logger.success('Data has been saved at {}', file_name)


# dates are re-formatted so that they are stored chronologically
def save_data_as_html(data, date):
    if data is None or not len(data) > 0:
        logger.error('HTML data is invalid for date {}', date)
        return False
    reformatted_date = re_format_date(date, '%d-%m-%Y', '%Y-%m-%d')
    file_name = '{}_{}.html'.format(file_locations.AMFI_HTML_DATA_FILE_PATH, reformatted_date)
    try:
        file_writer = open(file_name, 'w')
        file_writer.write(str(data))
        file_writer.close()
        logger.success('HTML data has been saved for {}', date)
        return True
    except Exception as e:
        logger.error('Faced error while saving html data for date {} \n {} - {}\n', date, type(e), e)
    return False


def save_test_nav_data(data):
    return split_and_save_combined_data(data, 'test nav')


def save_org_nav_data(data):
    return split_and_save_combined_data(data, 'org nav')


def update_list_of_all_schemes(data):
    existing_data_schemes_count = 0
    try:
        existing_data = get_list_of_all_schemes()
        existing_data_schemes_count = len(existing_data.index)
        data = data.append(existing_data, ignore_index=True).drop_duplicates()
    except FileNotFoundError:
        data = data.drop_duplicates()
    if len(data.index) < 44196:
        logger.critical('Some schemes are missing')
        return
    logger.debug('{} new schemes are present', len(data.index) - existing_data_schemes_count)
    data.sort_values(by=['Fund House Name', 'Scheme Name'], key=lambda x: x.apply(lambda y: y.lower()),
                     ignore_index=True, inplace=True)
    save_data_as_csv(data, file_locations.SCHEMES_LIST_FILE_PATH)


def get_file_name_from_year_and_type(file_type, year):
    label = get_file_type_label(file_type)
    return file_locations.PROCESSED_DATA_FOLDER_PATH + label + '_' + year


def split_and_save_combined_data(data, file_type, print_logs=True):
    try:
        data = sort_columns_date_wise(data)
        dates = data.columns.values[2:]
        years_set = sorted(set(map(lambda x: x[-4:], dates)))
        years_map = list(map(lambda x: x[-4:], dates))
        reversed_years_map = list(reversed(years_map))
        for year in years_set:
            scheme_name_data = data.iloc[:, :2]
            start_index = years_map.index(year) + 2
            end_index = len(years_map) - reversed_years_map.index(year) - 1 + 2
            date_wise_data = data.iloc[:, start_index:end_index + 1]
            data_for_year = scheme_name_data.join(date_wise_data)
            if print_logs:
                logger.info('{} has {} data points from {} till {}', year, len(data_for_year.columns.values) - 2,
                            data_for_year.columns.values[2], data_for_year.columns.values[-1])
            save_data_for_a_year(data_for_year, file_type, year, print_logs)
        return True
    except Exception as e:
        logger.error('Faced error while splitting data\n {} - {}\n', type(e), e)
    return False


def save_data_for_a_year(data, file_type, year, check_for_existing_data=True, print_logs=True):
    if check_for_existing_data:
        try:
            existing_data = get_data_for_a_year(file_type, year)
            if existing_data.equals(data):
                logger.info('Data for {} has not changed', year)
                return False
            if len(data.columns.values) < len(existing_data.columns.values):
                logger.critical('{} data frame for {} has lesser than existing columns', file_type, year)
                return False
            if len(data.index) < len(existing_data.index):
                logger.critical('{} data frame for {} has lesser than existing rows({}/{})', file_type, year,
                                len(data.index), len(existing_data.index))
                return False
            #  todo include non-NA count
        except FileNotFoundError:
            pass

    try:
        data = sort_columns_date_wise(data)
        if len(data.index) < 4:  # 44196:
            logger.critical('Some schemes were missing for {}', year)
            return False
        file_name = get_file_name_from_year_and_type(file_type, year)
        save_data_as_csv(data, file_name)
        return True
    except Exception as e:
        logger.error('Faced error while saving data for {}\n {} - {}\n', year, type(e), e)
    return False
