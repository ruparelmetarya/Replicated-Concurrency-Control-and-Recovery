"""
Author: Metarya Ruparel
email: msr9732@nyu.edu
Created On: 11/26/2021
"""
from Enums.abort_status import AbortStatus
from Enums.transaction_status import TransactionStatus
from Logger.logger import Logger

logger = Logger.get_logger(__name__)


class Transaction:
    def __init__(self, _id, _start_time, _read_only=False):
        self.ID = _id
        self.start_time = _start_time
        self.read_only = _read_only
        self.cache = {}
        self.commit_list = {}
        self.touch_set = set([])
        self.status = TransactionStatus.NORMAL
        self.query_buffer = []
        self.lock_list = {}
        self.abort = AbortStatus.FALSE

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
