"""
Author: Metarya Ruparel
email: msr9732@nyu.edu
Created On: 11/26/2021
"""
import collections

from Enums.lock_type import LockType


class Lock:
    def __init__(self):
        self._type = LockType.FREE
        self._lock_dict = collections.defaultdict(LockType)

    def add_lock(self, transaction_id, lock):
        if not isinstance(lock, LockType):
            raise TypeError('lock should be of type Enum LockType (FREE/READ/WRITE).')
        self._lock_dict[transaction_id] = lock
        self._type = lock

    def remove_lock(self, transaction_id):
        if self._lock_dict.get(transaction_id, None) is not None:
            self._lock_dict.pop(transaction_id)
        if not self._lock_dict:
            self._type = LockType.FREE

    @property
    def free(self):
        return self._type == LockType.FREE

    @property
    def type(self):
        return self._type

    @property
    def locks(self):
        return list(self._lock_dict.keys())
