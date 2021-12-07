"""
Author: Metarya Ruparel
email: msr9732@nyu.edu
Created On: 11/26/2021
"""
import collections

from Enums.abort_status import AbortStatus
from Enums.transaction_status import TransactionStatus
from Logger.logger import Logger

logger = Logger.get_logger(__name__)


class Transaction:
    def __init__(self, _id, _start_time, _read_only=False):
        self._ID = _id
        self._start_time = _start_time
        self._read_only = _read_only
        self._cache = collections.defaultdict()
        self._commit_list = collections.defaultdict()
        self._touch_set = set([])
        self._status = TransactionStatus.NORMAL
        self._query_buffer = []
        self._lock_list = collections.defaultdict()
        self._abort = AbortStatus.FALSE

    def dump_transaction_status(self):
        transaction_status = ""
        transaction_status += "T" + str(self.ID)
        if self.read_only:
            transaction_status += "\ttype: ro"
        else:
            transaction_status += "\ttype: rw"
        transaction_status += "\t|\tstart @ " + str(self.start_time)
        transaction_status += "\t|\tstatus : " + str(self.status)
        transaction_status += "\t|\ttouched_set: "
        for ts in self.touch_set:
            transaction_status += str(ts) + " "
        transaction_status += "\t|\tvariable_locked: "
        for ld in self.lock_list:
            transaction_status += str(ld) + " "
        return transaction_status

    @property
    def read_only(self):
        return self._read_only

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, new_status):
        self._status = new_status

    @property
    def touch_set(self):
        return self._touch_set

    @touch_set.setter
    def touch_set(self, s):
        self._touch_set |= s

    @property
    def lock_list(self):
        return self._lock_list

    def set_lock_list(self, key, val):
        self._lock_list[key] = val

    @property
    def query_buffer(self):
        return self._query_buffer

    @query_buffer.setter
    def query_buffer(self, value):
        self._query_buffer = value

    @property
    def commit_list(self):
        return self._commit_list

    def set_commit_list(self, key, val):
        self._commit_list[key] = val

    @property
    def abort(self):
        return self._abort

    @abort.setter
    def abort(self, value):
        self._abort = value

    @property
    def start_time(self):
        return self._start_time
