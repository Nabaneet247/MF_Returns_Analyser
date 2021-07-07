import multiprocessing as mp
from glob import glob

from icecream import ic
from loguru import logger

import file_locations
from amfi_service import get_html_data_from_amfi
from date_utils import get_valid_dates_for_a_year
from date_utils import re_format_date
from general_utils import find_difference_between_two_lists
from mf_data_saver import save_data_as_html


def fetch_and_save_html_data_from_amfi_for_a_date(date):
    html_data = get_html_data_from_amfi(date)
    save_data_as_html(data=html_data, date=date)
    return html_data


def get_list_dates_for_which_navs_have_been_fetched():
    list_of_files = glob(file_locations.AMFI_HTML_DATA_FOLDER_PATH + '*.html')
    collected_dates = list(map(lambda x: re_format_date(x[-15:-5], '%Y-%m-%d', '%d-%m-%Y'), list_of_files))
    return collected_dates


def fetch_and_save_html_data_from_amfi_for_multiple_dates(dates, max_processes=1):
    with mp.Pool(processes=max_processes) as process:
        process.map(fetch_and_save_html_data_from_amfi_for_a_date, dates)


def fetch_html_data_from_amfi_for_missing_dates_for_a_year(year_string):
    existing_dates = get_list_dates_for_which_navs_have_been_fetched()
    valid_dates = get_valid_dates_for_a_year(year_string)
    missing_dates = find_difference_between_two_lists(valid_dates, existing_dates)
    if len(missing_dates) > 0:
        ic(missing_dates[0], missing_dates[-1], len(missing_dates))
    else:
        logger.info('No dates are missing for {}', year_string)
    fetch_and_save_html_data_from_amfi_for_multiple_dates(missing_dates, max_processes=3)


if __name__ == '__main__':
    years = [str(x) for x in range(2006, 2022)]
    for year in years:
        fetch_html_data_from_amfi_for_missing_dates_for_a_year(year)
