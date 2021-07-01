import multiprocessing as mp

from loguru import logger

import file_locations
from amfi_data_processor import get_data_frame_from_html_code
from amfi_service import get_all_fund_house_and_scheme_names
from date_utils import get_valid_dates_for_a_year
from general_utils import find_difference_between_two_lists
from mf_data_collector import fetch_and_save_html_data_from_amfi_for_a_date
from mf_data_provider import get_data_for_a_year
from mf_data_provider import read_html_nav_data, get_list_of_all_valid_years
from mf_data_saver import save_data_for_a_year

log_filename = file_locations.PROCESSED_DATA_FOLDER_PATH + 'processor_logs.log'
logger_message_format = "{time} - {name} - {level} - {message}"
logger.add(log_filename, level="INFO", format=logger_message_format)
logger.add(log_filename, level="DEBUG", format=logger_message_format)
logger.add(log_filename, level="ERROR", format=logger_message_format)


def update_org_nav_files_for_a_year(year):
    valid_dates = get_valid_dates_for_a_year(year)
    try:
        data = get_data_for_a_year('org nav', year)
        data = get_all_fund_house_and_scheme_names(refetch_schemes_list_from_amfi=False).join(
            data.set_index(['Fund House Name', 'Scheme Name']),
            on=['Fund House Name', 'Scheme Name'])
    except FileNotFoundError:
        data = get_all_fund_house_and_scheme_names()
    existing_dates = data.columns.values[2:] if len(data.index) > 2 else []
    missing_dates = find_difference_between_two_lists(valid_dates, existing_dates)
    batch_size = 5
    counter = 1
    if len(missing_dates) == 0:
        logger.warning('No dates are missing for {}', year)
        return
    for date in missing_dates:
        html_code = read_html_nav_data(date)
        if html_code is None or not len(html_code) > 0:
            html_code = fetch_and_save_html_data_from_amfi_for_a_date(date)
        nav_data = get_data_frame_from_html_code(html_code, date)
        data = data.join(nav_data.set_index(['Fund House Name', 'Scheme Name']),
                         on=['Fund House Name', 'Scheme Name'])
        counter += 1
        if counter >= batch_size:
            counter = 1
            if save_data_for_a_year(data, 'org nav', year, check_for_existing_data=False):
                logger.warning('NAV data for {} was saved up to {}', year, date)
    save_data_for_a_year(data, 'org nav', year, check_for_existing_data=False)


def update_all_org_nav_files(max_processes=1):
    years = get_list_of_all_valid_years()
    with mp.Pool(processes=max_processes) as process:
        process.map(update_org_nav_files_for_a_year, years)


if __name__ == '__main__':
    # get_all_fund_house_and_scheme_names(refetch_schemes_list_from_amfi=True)
    # update_all_org_nav_files(1)
    pass
