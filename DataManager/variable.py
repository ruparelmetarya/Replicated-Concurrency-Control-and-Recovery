"""
Author: Metarya Ruparel
email: msr9732@nyu.edu
Created On: 11/26/2021
"""


class Variable:
    def __init__(self, _num):
        self._value = _num * 10
        self._idx = _num
        self._ID = 'x' + str(_num)

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, new_value):
        self._value = new_value

    @property
    def idx(self):
        return self._idx

    @property
    def id(self):
        return self._ID
