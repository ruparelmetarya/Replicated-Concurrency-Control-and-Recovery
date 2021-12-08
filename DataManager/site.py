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


class Site:
    def __init__(self, site_num):
        self._is_running = True
        self._is_recovered = True
        self._site_num = site_num
        # key: VariableID, value: Variable, this is for fast query data by ID
        self._variables = collections.defaultdict(Variable)
        self._lock_table = collections.defaultdict(Lock)
        # key: VariableID, value: boolean, ready or not
        # Note this isReady is to determine whether a variable's value is valid for read.
        self._is_ready = collections.defaultdict(bool)
        self._timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # initialize the site here.
        for val in range(1, 21):
            new_variable = Variable(val)
            if val % 2 == 0 or (val + 1) % 10 == self.site_num:
                self.set_variable("x" + str(val), new_variable)
                self.set_lock_table("x" + str(val), Lock())
                self.set_is_ready("x" + str(val), True)

    def variable(self, variable_id):
        return self._variables[variable_id]

    def get_lock_type(self, lock_id):
        if self._lock_table.get(lock_id, None) is not None:
            return self._lock_table.get(lock_id).type
        raise ValueError(f'Lock with id %s does not exist.'.format(str(lock_id)))

    def is_variable_free(self, variable_id):
        return self._lock_table.get(variable_id, LockType.FREE)

    def is_valid_variable(self, variable_id):
        return self._is_ready.get(variable_id, False)

    @staticmethod
    def is_replicated(variable_id):
        value = int(variable_id[1:])
        if value % 2 == 0:
            return True
        return False

    def add_variable_lock(self, variable_id, transaction_id, lock_type):
        if self._lock_table.get(variable_id, None) is not None:
            self._lock_table.get(variable_id).add_lock(transaction_id, lock_type)

    # remove locks by a certain transaction
    def remove_variable_lock(self, variable_id, transaction_id):
        if self._lock_table.get(variable_id, None) is not None:
            self._lock_table[variable_id].remove_lock(transaction_id)

    def write_to_variable(self, variable_id, val):
        self.variables[variable_id].value = val

    def fail_site(self):
        print("Failing site ", self.site_num)
        self.is_running = False
        self.is_recovered = False
        self.clear_lock_table()
        for var in self.is_ready:
            self.set_is_ready(var, False)

    # recover the site and initialize the lock table
    # determine whether the variables are valid for read or not
    def recover_site(self):
        # reset timestamp:
        self.timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.is_running = True
        for ID in self.variables:
            self.set_lock_table(ID, Lock())
        # note the isReplicated() here!
        for ID in self.is_ready:
            if not self.is_replicated(ID):
                self.set_is_ready(ID, True)
            else:
                self.set_is_ready(ID, False)

    def clear_lock_table(self):
        self._lock_table.clear()

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, new_ts):
        self._timestamp = new_ts

    @property
    def variables(self):
        return self._variables

    def set_variable(self, variable_id, variable):
        self._variables[variable_id] = variable

    @property
    def site_num(self):
        return self._site_num

    @property
    def is_running(self):
        return self._is_running

    @is_running.setter
    def is_running(self, status):
        self._is_running = status

    @property
    def is_recovered(self):
        return self._is_recovered

    @is_recovered.setter
    def is_recovered(self, status):
        self._is_recovered = status

    @property
    def is_ready(self):
        return self._is_ready

    def set_is_ready(self, variable_id, status):
        self._is_ready[variable_id] = status

    @property
    def lock_table(self):
        return self._lock_table

    def set_lock_table(self, variable_id, lock):
        self._lock_table[variable_id] = lock
