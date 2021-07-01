import sys
from datetime import datetime

from amfi_service import get_all_fund_house_and_scheme_names
from mf_data_cleaner import clean_all_nav_data
from mf_data_files_updater import update_schemes_in_all_data_files
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
        fetch_attempts += 1
        if fetch_attempts > 5:
            sys.exit(1)
        max_tries_per_house += 5
        new_schemes_list = get_all_fund_house_and_scheme_names(refetch_schemes_list_from_amfi=True)

    update_schemes_in_all_data_files()
    current_year = str(datetime.now().year)
    update_org_nav_files_for_a_year(current_year)
    clean_all_nav_data()
    intervals = ['30D', '3M', '6M', '1Y', '2Y', '5Y']
    update_returns_for_intervals(intervals=intervals, update_latest_returns_only=True)
    analyse_returns_for_intervals(intervals=intervals)
    today_date = datetime.now().strftime('%d-%m-%Y')
    plot_analysed_data_file_for_a_date(today_date)
