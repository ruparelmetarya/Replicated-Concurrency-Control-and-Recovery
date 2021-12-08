"""
Author: Metarya Ruparel
email: msr9732@nyu.edu
Created On: 11/26/2021
"""

from enum import Enum


class LockType(Enum):
    """
    Represents the type of a Lock.
    """

    FREE = 0
    READ = 1
    WRITE = 2
