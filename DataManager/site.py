"""
Author: Metarya Ruparel
email: msr9732@nyu.edu
Created On: 11/26/2021
"""
import collections
from datetime import datetime

from DataManager.variable import Variable
from DataManager.lock import Lock
from Enums.lock_type import LockType
from Logger.logger import Logger

LOGGER = Logger.get_logger(__name__)


class Site:
    """
    This class represents a Site in the system.
    """

    def __init__(self, site_num):
        self._is_running = True
        self._is_recovered = True
        self._site_num = site_num
        self._variables = collections.defaultdict(Variable)
        self._lock_table = collections.defaultdict(Lock)
        self._is_ready = collections.defaultdict(bool)
        self._timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for val in range(1, 21):
            new_variable = Variable(val)
            if val % 2 == 0 or (val + 1) % 10 == self.site_num:
                self.set_variable("x" + str(val), new_variable)
                self.set_lock_table("x" + str(val), Lock())
                self.set_is_ready("x" + str(val), True)

    def variable(self, variable_id):
        """
        Retrieve the Variable using the ID.
        :param variable_id: ID of the variable.
        :return: Variable
        """
        return self._variables[variable_id]

    def get_lock_type(self, lock_id):
        """
        Retrieve the LockType using the Lock ID.
        :param lock_id: ID of the Lock
        :return: LockType Enum
        """
        if self._lock_table.get(lock_id, None) is not None:
            return self._lock_table.get(lock_id).type
        error_msg = "Lock with id {ID} does not exist.".format(ID=str(lock_id))
        LOGGER.error(error_msg)
        raise ValueError(error_msg)

    def is_variable_free(self, variable_id):
        """
        Check if a given variable is free or not.
        :param variable_id: ID of the variable.
        :return: LockType Enum
        """
        return self._lock_table.get(variable_id, LockType.FREE)

    def is_valid_variable(self, variable_id):
        """
        Check if a given variable is valid or not.
        :param variable_id: ID of the variable.
        :return: Bool
        """
        return self._is_ready.get(variable_id, False)

    @staticmethod
    def is_replicated(variable_id):

        """
        Check if a given variable is replicated.
        :param variable_id: ID of the variable.
        :return: Bool
        """
        value = int(variable_id[1:])
        if value % 2 == 0:
            return True
        return False

    def add_variable_lock(self, variable_id, transaction_id, lock_type):
        """
        Lock a given variable.
        :param variable_id: ID of the variable.
        :param transaction_id: ID of the transaction locking the variable.
        :param lock_type: LockType of the lock to be acquired.
        :return: None
        """
        LOGGER.debug("Acquiring lock " + str(lock_type) + "on variable " + str(variable_id))
        if self._lock_table.get(variable_id, None) is not None:
            self._lock_table.get(variable_id).add_lock(transaction_id, lock_type)

    def remove_variable_lock(self, variable_id, transaction_id):
        """
        Remove lock from a given variable.
        :param variable_id: ID of the variable.
        :param transaction_id: ID of the transaction locking the variable.
        :return: None
        """
        LOGGER.debug("Removing lock from the variable " + str(variable_id))
        if self._lock_table.get(variable_id, None) is not None:
            self._lock_table[variable_id].remove_lock(transaction_id)

    def write_to_variable(self, variable_id, val):
        """
        Write new value to a given variable.
        :param variable_id: ID of the variable.
        :param val: new value to be written.
        :return: None
        """
        LOGGER.debug("Writing value " + str(val) + "to variable " + str(variable_id))
        self.variables[variable_id].value = val

    def fail_site(self):
        """
        Fail a given site.
        :return: None
        """
        LOGGER.debug("Failing site " + str(self.site_num))
        self.is_running = False
        self.is_recovered = False
        self.clear_lock_table()
        for var in self.is_ready:
            self.set_is_ready(var, False)

    def recover_site(self):
        """
        Recover a given site.
        :return: None
        """
        LOGGER.debug("Recovering site " + str(self.site_num))
        self.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.is_running = True
        for ID in self.variables:
            self.set_lock_table(ID, Lock())
        for ID in self.is_ready:
            if not self.is_replicated(ID):
                self.set_is_ready(ID, True)
            else:
                self.set_is_ready(ID, False)

    def clear_lock_table(self):
        """
        Used to clear all locks held.
        """
        self._lock_table.clear()

    @property
    def timestamp(self):
        """
        Property for accessing private class member _timestamp.
        """
        return self._timestamp

    @timestamp.setter
    def timestamp(self, new_ts):
        """
        Set a value for private member _timestamp.
        """
        self._timestamp = new_ts

    @property
    def variables(self):
        """
        Property for accessing private class member _variables.
        """
        return self._variables

    def set_variable(self, variable_id, variable):
        """
        Set a value for private member _variables.
        """
        self._variables[variable_id] = variable

    @property
    def site_num(self):
        """
        Property for accessing private class member _site_num.
        """
        return self._site_num

    @property
    def is_running(self):
        """
        Property for accessing private class member _is_running.
        """
        return self._is_running

    @is_running.setter
    def is_running(self, status):
        """
        Set a value for private member _is_running.
        """
        self._is_running = status

    @property
    def is_recovered(self):
        """
        Property for accessing private class member _is_recovered.
        """
        return self._is_recovered

    @is_recovered.setter
    def is_recovered(self, status):
        """
        Set a value for private member _is_recovered.
        """
        self._is_recovered = status

    @property
    def is_ready(self):
        """
        Property for accessing private class member _is_ready.
        """
        return self._is_ready

    def set_is_ready(self, variable_id, status):
        """
        Update ready status of a variable.
        """
        self._is_ready[variable_id] = status

    @property
    def lock_table(self):
        """
        Property for accessing private class member _lock_table.
        """
        return self._lock_table

    def set_lock_table(self, variable_id, lock):
        """
        Set a lock in lock table.
        """
        self._lock_table[variable_id] = lock
