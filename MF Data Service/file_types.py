file_types = {'org nav': 'Original NAV Data',
              'cln nav': 'Cleaned NAV Data',
              'test nav': 'Test NAV Data',
              '1Y ret': '1 Year Returns',
              '2Y ret': '2 Year Returns',
              '5Y ret': '5 Year Returns',
              '30D ret': '30 Day Returns',
              '3M ret': '3 Month Returns',
              '6M ret': '6 Month Returns'
              }


def get_file_type_keys():
    return file_types.keys()


def get_file_type_label(file_type):
    return file_types[file_type]
