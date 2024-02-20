import datetime
import os

brazilian_holidays_2024 = ['2024-01-01', '2024-02-12', '2024-02-13', '2024-03-29', '2024-05-01', '2024-05-30',
                           '2024-11-15', '2024-11-20', '2023-12-25']


def get_dir_path(file_name):
    source_dir_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(source_dir_path, file_name)


def read_from_file(file_name, property_name):
    with open(file_name) as f:
        for line in f.readlines():
            if property_name in line:
                property_value = line.split(":")[1].strip()
                return property_value


def get_date_from_string(date_str, format='%d/%m/%Y'):
    return datetime.datetime.strptime(date_str, format)


def date_is_business_day(date):
    if date.strftime("%A") == 'Saturday' or date.strftime("%A") == 'Sunday':
        return False
    return not date_is_holiday(date)


def date_is_holiday(date):
    date_str = date.strftime("%Y-%m-%d")
    return date_str in brazilian_holidays_2024


def is_index_asset(asset_code):
    return asset_code.startswith('WIN') or asset_code.startswith('IND')


def str_to_bool(s):
    s = s.lower().strip()  # Convert to lowercase and remove leading/trailing spaces
    if s == 'yes' or s == 'true':
        return True
    else:
        return False
