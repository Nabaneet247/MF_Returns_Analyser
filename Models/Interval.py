class Interval:
    def __init__(self):
        self._unit = None
        self._value = None
        self._offset = None
        self._change_rate_label = None
        self._abbreviation = None
        self._max_dates_padded = None

    @property
    def unit(self):
        return self._unit

    @property
    def value(self):
        return self._value

    @property
    def offset(self):
        return self._offset

    @property
    def change_rate_label (self):
        return self._change_rate_label

    @property
    def abbreviation(self):
        return self._abbreviation

    @property
    def max_dates_padded(self):
        return self._max_dates_padded

    @property
    def file_type(self):
        return self._abbreviation + ' ret'

    @unit.setter
    def unit(self, unit_str):
        self._unit = unit_str

    @value.setter
    def value(self, value_int):
        self._value = value_int

    @offset.setter
    def offset(self, offset_obj):
        self._offset = offset_obj

    @change_rate_label.setter
    def change_rate_label(self, label):
        self._change_rate_label = label

    @abbreviation.setter
    def abbreviation(self, abbreviation):
        self._abbreviation = abbreviation

    @max_dates_padded.setter
    def max_dates_padded(self, max_dates_padded):
        self._max_dates_padded = max_dates_padded
