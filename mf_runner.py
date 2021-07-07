import sys
from datetime import datetime

from icecream import ic

from amfi_service import get_all_fund_house_and_scheme_names
from mf_data_cleaner import clean_all_nav_data
from mf_data_collector import fetch_html_data_from_amfi_for_missing_dates_for_a_year
from mf_data_files_updater import update_schemes_in_data_file
from mf_data_generator import update_org_nav_files_for_a_year
from mf_data_plotter import plot_analysed_data_file_for_a_date
from mf_returns_analyser import analyse_returns_for_intervals
from mf_returns_generator import update_returns_for_intervals


def run():
    existing_schemes_list = get_all_fund_house_and_scheme_names()
    max_tries_per_house = 20
    new_schemes_list = get_all_fund_house_and_scheme_names(refetch_schemes_list_from_amfi=True,
                                                           max_tries_per_house=max_tries_per_house)
    fetch_attempts = 1
    while len(new_schemes_list.index) < len(existing_schemes_list.index):
        ic(len(new_schemes_list.index))
        ic(len(existing_schemes_list.index))
        fetch_attempts += 1
        if fetch_attempts > 5:
            sys.exit(1)
        max_tries_per_house += 5
        new_schemes_list = get_all_fund_house_and_scheme_names(refetch_schemes_list_from_amfi=True)

    if len(new_schemes_list.index) > len(existing_schemes_list.index):
        update_schemes_in_data_file('org nav')

    # todo handle start of new year scenario
    current_year = str(datetime.now().year)
    fetch_html_data_from_amfi_for_missing_dates_for_a_year(current_year)
    update_org_nav_files_for_a_year(current_year)
    # todo check cleanliness of latest data first
    clean_all_nav_data()
    intervals = ['30D', '3M', '6M', '1Y', '2Y', '5Y']
    # todo improve efficiency
    update_returns_for_intervals(intervals=intervals, update_latest_returns_only=True)
    last_date_analysed = analyse_returns_for_intervals(intervals=intervals)
    plot_analysed_data_file_for_a_date(last_date_analysed)


if __name__ == '__main__':
    run()
