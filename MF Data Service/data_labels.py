from Interval import Interval

statistic_labels = {'count': '-Data Points Count', 'mean': '-Mean', 'std': '-Std Dev', 'last ret': '-Last Return', 'min': '-Minimum',
                    '10': '-10th Percentile', '50': '-50th Percentile', '90': '-90th Percentile',
                    'max': '-Maximum', 'v score': '-Volatility Score'}

common_labels = {'last nav': 'Last NAV Value',
                 'first date': 'First Available Date',
                 'last date': 'Last Available Date',
                 'nav count': 'NAV Data Points Count',
                 'scheme active': 'Scheme Currently Active'}


def get_stat_label_for_key(key, interval: Interval):
    return interval.abbreviation + ' Returns' + statistic_labels[key]


def get_common_label_for_key(key):
    return common_labels[key]


def get_all_stat_labels_for_an_interval(interval: Interval):
    return [interval.abbreviation + ' Returns' + statistic_labels[key] for key in statistic_labels.keys()]


def get_common_label_values():
    return common_labels.values()
