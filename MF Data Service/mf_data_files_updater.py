from loguru import logger

from file_types import get_file_type_keys
from mf_data_provider import get_combined_year_wise_data
from mf_data_saver import split_and_save_combined_data


def update_schemes_in_data_file(file_type):
    data = get_combined_year_wise_data(file_type)
    if len(data.columns.values) < 3:
        logger.info('{} files are not present', file_type)
        return
    split_and_save_combined_data(data=data, file_type=file_type)
    logger.info('{} files were updated', file_type)


def update_schemes_in_all_data_files():
    for file_type in get_file_type_keys():
        update_schemes_in_data_file(file_type)
