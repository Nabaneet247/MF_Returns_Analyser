import re

invalid_test_strings = [' 4 2year', '55.2', 's55', '0', '1', 'sad 4 sad', '20', '20yr', '15days', '2143324 d',
                        '0 start_year', '1 month day', '10 DAy']
valid_test_strings = ['4Y', '3 months', '25 DAYS']
starting_number_pattern = '^([1-9]\d*)'
number_pattern = '(\d+)'
character_pattern = '(y|m|d|years?|yrs?|months?|days?)$'


def get_period_labels_from_string(string):
    string = string.strip().lower()
    match = re.search(starting_number_pattern, string)
    if match:
        number = int(match.group())
        remaining_string = string[match.end():].strip()

        if re.search(number_pattern, remaining_string):
            raise ValueError(string + ' is not a valid frequency. ' + 'Frequency value should start with a number.' +
                             '\nUse values like 1Y, 3M, 30D')

        match = re.search(character_pattern, remaining_string)

        if match and len(match.groups()) == 1 and match.start() == 0:
            return number, match.group()
        else:
            raise ValueError(string + ' is not a valid frequency. ' + 'Frequency value should start with a number.' +
                             '\nUse values like 1Y, 3M, 30D')
    else:
        raise ValueError(string + ' is not a valid frequency. ' + 'Frequency value should start with a number.' +
                         '\nUse values like 1Y, 3M, 30D')
