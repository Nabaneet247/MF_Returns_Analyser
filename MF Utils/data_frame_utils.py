import math

import pandas as pd
from loguru import logger

from general_utils import find_difference_between_two_lists


def sort_columns_date_wise(data):
    return data.iloc[:, :2].join(
        data.iloc[:, 2:].sort_index(axis=1, key=lambda x: pd.to_datetime(x, format='%d-%m-%Y')))


def check_for_schemes_mismatch_in_data_frames(data1, data2):
    if len(data1.index) != len(data2.index):
        return False
    schemes1 = []
    for index, row in data1.iterrows():
        schemes1.append(row['Fund House Name'] + '_' + row['Scheme Name'])
    schemes2 = []
    for index, row in data2.iterrows():
        schemes2.append(row['Fund House Name'] + '_' + row['Scheme Name'])
    return len(find_difference_between_two_lists(schemes1, schemes2)) > 0


def apply_func_to_data_frame_based_on_index(data_frame, func, start_r_index, end_r_index, start_c_index, end_c_index,
                                            include_last_row=False, include_last_column=False):
    if include_last_row:
        end_r_index += 1

    if include_last_column:
        end_r_index += 1

    column_count = len(data_frame.columns.values)
    row_count = len(data_frame.index)

    if not (0 <= start_c_index <= column_count and 0 <= end_c_index <= column_count):
        logger.error('Invalid column indexes received. Start: {}, End: {}, Length: {}', start_c_index, end_c_index,
                     column_count)
        return False

    if not (0 <= start_r_index <= row_count and 0 <= end_r_index <= row_count):
        logger.error('Invalid row indexes received. Start: {}, End: {}, Length: {}', start_r_index, end_r_index,
                     row_count)
        return False

    for i in range(start_r_index, end_r_index):
        for j in range(start_c_index, end_c_index):
            try:
                if not pd.isna(data_frame.iloc[i, j]):
                    data_frame.iloc[i, j] = func(data_frame.iloc[i, j])
            except Exception as e:
                logger.error('Got error while applying func at the indexes ({}, {})\nError: {} - {}', i, j, type(e), e)
                return False
    return True


def apply_func_to_data_frame(data_frame, func, start_r_name, end_r_name, start_c_name, end_c_name,
                             include_last_row=False, include_last_column=False):
    columns_indexes = data_frame.columns.values.tolist()
    row_indexes = data_frame.index.values.tolist()
    try:
        start_r_index = row_indexes.index(start_r_name)
        end_r_index = row_indexes.index(end_r_name)
        start_c_index = columns_indexes.index(start_c_name)
        end_c_index = columns_indexes.index(end_c_name)
        return apply_func_to_data_frame_based_on_index(data_frame, func,
                                                       start_r_index, end_r_index,
                                                       start_c_index, end_c_index,
                                                       include_last_row=include_last_row,
                                                       include_last_column=include_last_column)

    except ValueError:
        logger.error(
            'Invalid row/column values received\nStart Row:{}, End Row:{}, Include Last:{}\nStart Column:{}, End Column:{}, Include Last:{}',
            start_r_name, end_r_name, include_last_row, start_c_name, end_c_name, include_last_column)
    return False


def convert_data_frame_to_list_of_chunks_row_wise(data, chunks_count):
    chunk_size = int(math.ceil(len(data.index) / chunks_count))
    chunks = []
    for i in range(chunks_count):
        start_row_index = i * chunk_size
        end_row_index = (i + 1) * chunk_size
        if end_row_index > len(data.index):
            end_row_index = len(data.index)
        # ic(i+1, start_row_index, end_row_index)
        if start_row_index >= len(data.index):
            break
        chunks.append(data.iloc[start_row_index:end_row_index])
    # ic(len(chunks))
    return chunks
