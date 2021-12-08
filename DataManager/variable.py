"""
Author: Metarya Ruparel
email: msr9732@nyu.edu
Created On: 11/26/2021
"""


class Variable:
    """
    Represents a variable in the system.
    """
    def __init__(self, _num):
        self._value = _num * 10
        self._idx = _num
        self._ID = 'x' + str(_num)

    @property
    def value(self):
        """
        Property for accessing private class variable _value.
        """
        return self._value

    @value.setter
    def value(self, new_value):
        """
        Property for setting a new value to a private class variable _value.
        """
        self._value = new_value

    @property
    def idx(self):
        """
        Property for accessing private class variable _idx.
        """
        return self._idx

    @property
    def id(self):
        """
        Property for accessing private class variable _ID.
        """
        return self._ID
