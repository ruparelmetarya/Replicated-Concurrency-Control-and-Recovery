"""
Author: Metarya Ruparel
email: msr9732@nyu.edu
Created On: 11/26/2021
"""
import collections

from Enums.lock_type import LockType
from Logger.logger import Logger

LOGGER = Logger.get_logger(__name__)


class Lock:
    """
    Implements the locking interface used for locking transactions.
    """

    def __init__(self):
        self._type = LockType.FREE
        self._lock_dict = collections.defaultdict(LockType)

    def add_lock(self, transaction_id, lock):
        """
        Add lock to a transaction.
        :param transaction_id: ID of the transaction.
        :param lock: Enum type of the lock.
        :return: None
        """
        if not isinstance(lock, LockType):
            error_msg = "Lock should be of type Enum LockType (FREE/READ/WRITE)."
            LOGGER.error(error_msg)
            raise TypeError(error_msg)
        LOGGER.debug("Adding lock " + str(lock) + " to transaction " + transaction_id)
        self._lock_dict[transaction_id] = lock
        LOGGER.debug("Updating lock type to " + str(lock))
        self._type = lock

    def remove_lock(self, transaction_id):
        """
        Remove lock from a transaction.
        :param transaction_id: ID of the transaction.
        :return: None
        """
        LOGGER.debug("Removing lock from transaction " + transaction_id)
        if self._lock_dict.get(transaction_id, None) is not None:
            self._lock_dict.pop(transaction_id)
        LOGGER.debug("Updated lock type to " + str(LockType.FREE))
        if not self._lock_dict:
            self._type = LockType.FREE

    @property
    def free(self):
        """
        Property for checking whether a lock is free or not.
        :return: Bool
        """
        return self._type == LockType.FREE

    @property
    def type(self):
        """
        Property for accessing private variable _type.
        :return: None
        """
        return self._type

    @property
    def locks(self):
        """
        Property of accessing all the locks held.
        :return: List
        """
        return list(self._lock_dict.keys())
