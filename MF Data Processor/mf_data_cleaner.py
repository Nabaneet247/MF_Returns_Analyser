import pandas as pd
from icecream import ic
from loguru import logger

import file_locations
from data_frame_utils import apply_func_to_data_frame
from data_frame_utils import convert_data_frame_to_list_of_chunks_row_wise
from general_utils import get_log_base_10
from mf_data_provider import get_org_nav_data
from mf_data_saver import split_and_save_combined_data

pd.set_option("display.max_rows", None, "display.max_columns", None)
log_filename = file_locations.PROCESSED_DATA_FOLDER_PATH + 'cleaner_logs.log'
logger_message_format = "{time} - {name} - {level} - {message}"
logger.add(log_filename, level="INFO", format=logger_message_format)
logger.add(log_filename, level="DEBUG", format=logger_message_format)
logger.add(log_filename, level="WARNING", format=logger_message_format)
logger.add(log_filename, level="ERROR", format=logger_message_format)


@logger.catch
def clean_nav_data(data):
    row_count = len(data.index)
    counter = 1
    schemes_cleaned = 0
    cleaning_errors = 0
    start_column_name = data.columns.values[2]
    end_column_name = data.columns.values[-1]
    ic(row_count, start_column_name, end_column_name)
    for index, data_row in data.iterrows():
        log_values_row = data_row[2:].apply(func=get_log_base_10)
        # ic(counter, row_count)
        counter += 1
        nav_log_data_differences = log_values_row.iloc[2:].dropna().diff()
        log2 = get_log_base_10(2)
        for i, value in nav_log_data_differences.dropna().items():
            if value >= log2:
                fund_name = data.loc[index]['Fund House Name']
                scheme_name = data.loc[index]['Scheme Name']
                date = i
                factor = round(10 ** value)

                if apply_func_to_data_frame(data, lambda x: x * factor, start_r_name=index, end_r_name=index,
                                            include_last_row=True, start_c_name=start_column_name, end_c_name=i):
                    cleaned = 'Yes'
                    schemes_cleaned += 1
                else:
                    cleaned = 'No'
                    cleaning_errors += 1

                logger.info(
                    'Excel Row Number: {}, Date: {}, Factor: {}, Cleaned: {}\nFund House: {}\nScheme Name: {}',
                    index + 2, date, factor, cleaned,
                    fund_name, scheme_name)

    if cleaning_errors > 0:
        logger.warning('{} schemes could not be cleaned', cleaning_errors)
        return False, data

    if schemes_cleaned >= 0:
        logger.success('{} schemes were cleaned', schemes_cleaned)
    return True, data


def clean_all_nav_data():
    chunks_count = 10
    data_chunks = convert_data_frame_to_list_of_chunks_row_wise(get_org_nav_data(), chunks_count=chunks_count)
    cleaned_data = pd.DataFrame()

    for chunk in data_chunks:
        start_row_index = chunk.index.values[0]
        end_row_index = chunk.index.values[-1]
        logger.debug('Gonna clean for rows {} - {}', start_row_index, end_row_index)
        cleaning_status, result = clean_nav_data(chunk)
        if not cleaning_status:
            return
        cleaned_data = cleaned_data.append(result)
    split_and_save_combined_data(cleaned_data, file_type='cln nav')


if __name__ == '__main__':
    clean_all_nav_data()
