"""
Author: Metarya Ruparel
email: msr9732@nyu.edu
Created On: 11/26/2021
"""

import collections
import re

from constants import BEGIN, BEGIN_RO, DUMP, END, FAIL, OPERATION_ERROR_MESSAGE, R, RECOVER, W
from DataManager.data_manager import DataManager
from Enums.abort_status import AbortStatus
from Enums.transaction_status import TransactionStatus
from Logger.logger import Logger
from TransactionManager.transaction import Transaction

LOGGER = Logger.get_logger(__name__)


class TransactionManager:
    """
    Transaction Manager translates read and write requests on variables to read and write requests on
    copies using the available copy algorithm.
    """

    def __init__(self):
        self.DM = DataManager()
        self.transaction_list = collections.defaultdict(Transaction)
        self.transaction_wait_table = collections.defaultdict(set)
        self.data_wait_table = collections.defaultdict(list)
        self.block_table = collections.defaultdict(list)
        self.fail_history = collections.defaultdict(list)
        self.final_result = collections.defaultdict()
        self.commit_summary = collections.defaultdict()

    @staticmethod
    def read_file(filename):
        """
        Open an input file.
        :param filename: Name of the file
        :return: List: Lines of the input file.
        """
        LOGGER.debug("Opening file {filename}".format(filename=filename))
        try:
            infile = open(filename, 'r')
            return infile.readlines()
        except IOError:
            LOGGER.error("Error while opening file {filename}".format(filename=filename))
            raise IOError

    def parser(self, filename):
        """
        This is the main crux of the Transaction Manager. We read input file line by line here and request
        read/write on variables based on the input specification.
        :param filename: Name of the input file
        :return: None
        """
        LOGGER.debug("Reading file {filename}".format(filename=filename))
        data = self.read_file(filename=filename)
        line_num = time = 0
        for line in data:
            line_num += 1
            time += 1
            LOGGER.info("Current Time: {time}".format(time=time))
            self.deadlock_detection(time)
            line = line.strip('\n')
            if line[0] in "/#'\"":
                continue
            operation = re.split('[()]', line)
            operation_name = operation[0].strip()

            if len(operation) < 2:
                errmsg = "error: missing parameters for [" + operation_name + "] in line " + str(line_num)
                raise ValueError(errmsg)
            else:
                operation_arg = re.split(',', operation[1])

            if operation_name == BEGIN:
                if len(operation_arg) != 1:
                    error_msg = OPERATION_ERROR_MESSAGE.format(line_num=line, OP=BEGIN, n=1)
                    LOGGER.error(error_msg)
                    raise ValueError(error_msg)
                transaction_id = int(operation_arg[0].strip()[1:])
                self.begin(transaction_id, time)

            elif operation_name == BEGIN_RO:
                if len(operation_arg) != 1:
                    error_msg = OPERATION_ERROR_MESSAGE.format(line_num=line, OP=BEGIN_RO, n=1)
                    LOGGER.error(error_msg)
                    raise ValueError(error_msg)
                transaction_id = int(operation_arg[0].strip()[1:])
                self.begin(transaction_id, time, read_only=True)

            elif operation_name == R:
                if len(operation_arg) != 2:
                    error_msg = OPERATION_ERROR_MESSAGE.format(line_num=line, OP=R, n=2)
                    LOGGER.error(error_msg)
                    raise ValueError(error_msg)
                transaction_id = int(operation_arg[0].strip()[1:])
                variable_id = operation_arg[1].strip()
                self.read(transaction_id, variable_id, time)

            elif operation_name == W:
                if len(operation_arg) != 3:
                    error_msg = OPERATION_ERROR_MESSAGE.format(line_num=line, OP=W, n=3)
                    raise ValueError(error_msg)
                transaction_id = int(operation_arg[0].strip()[1:])
                variable_id = operation_arg[1].strip()
                value = int(operation_arg[2].strip())
                self.write(transaction_id, variable_id, value)

            elif operation_name == DUMP:
                if len(operation_arg) != 1:
                    error_msg = OPERATION_ERROR_MESSAGE.format(line_num=line, OP=DUMP, n=1)
                    raise ValueError(error_msg)
                if len(operation_arg[0]) == 0:
                    self.dump()
                elif operation_arg[0][0] == 'x':
                    variable_id = operation_arg[0].strip()
                    self.dump(variable=variable_id)
                else:
                    site_id = int(operation_arg[0].strip())
                    self.dump(site=site_id)

            elif operation_name == END:
                if len(operation_arg) != 1:
                    error_msg = OPERATION_ERROR_MESSAGE.format(line_num=line, OP=END, n=1)
                    raise ValueError(error_msg)
                transaction_id = int(operation_arg[0].strip()[1:])
                self.end(transaction_id, time)

            elif operation_name == FAIL:
                if len(operation_arg) != 1:
                    error_msg = OPERATION_ERROR_MESSAGE.format(line_num=line, OP=FAIL, n=1)
                    raise ValueError(error_msg)
                site_id = int(operation_arg[0].strip())
                self.fail(site_id, time)

            elif operation_name == RECOVER:
                if len(operation_arg) != 1:
                    error_msg = OPERATION_ERROR_MESSAGE.format(line_num=line, OP=RECOVER, n=1)
                    raise ValueError(error_msg)
                site_id = int(operation_arg[0].strip())
                self.recover(site_id)

            else:
                error_msg = OPERATION_ERROR_MESSAGE.format(line_num=line, OP='[' + operation_name + ']', n=1)
                raise ValueError(error_msg)

            # self.print_status()

    def begin(self, transaction_id, time, read_only=False):
        """
        Begin a given transaction.
        :param transaction_id: ID of the transaction.
        :param time: Current time of the system.
        :param read_only: boolean indicating whether the Transaction is read_only or not.
        :return: None
        """
        msg = "begin T" + str(transaction_id)
        if read_only:
            msg += "(read-only)"
        msg += " @ tick " + str(time)
        LOGGER.info(msg)
        transaction = Transaction(_id=transaction_id, _start_time=time, _read_only=read_only)
        self.transaction_list[transaction_id] = transaction

    def read(self, transaction_id, variable_id, time):
        """
        Read the variable for a given transaction. This internally calls Data Managers read.
        :param transaction_id: ID of the transaction.
        :param variable_id: ID of the variable.
        :param time: Current time of the system.
        :return: None
        """
        LOGGER.info("T" + str(transaction_id) + " requested to read " + str(variable_id))
        read_only = self.transaction_list.get(transaction_id).read_only
        LOGGER.debug("Calling the Data Manager to read " + str(variable_id))
        read_result = self.DM.read(self.transaction_list.get(transaction_id), variable_id)

        if read_result[0]:
            LOGGER.info("Read successful. Written to site(s): " + read_result[1].__str__())
            if not read_only:
                sites_touched = set(read_result[1])
                self.transaction_list.get(transaction_id).touch_set = sites_touched
                self.transaction_list.get(transaction_id).status = TransactionStatus.NORMAL
                if self.transaction_list.get(transaction_id).lock_list.get(variable_id, None) is None:
                    self.transaction_list.get(transaction_id).set_lock_list(variable_id, 'r')
                if self.transaction_wait_table.get(transaction_id, None) is not None:
                    del self.transaction_wait_table[transaction_id]
        else:
            LOGGER.info("Write unsuccessful. Blocked by site(s): " + read_result[1].__str__())
            if read_only and read_result[1] == -1:
                blocker = -1
                self.transaction_wait_table[transaction_id].add(blocker)
                self.block_table[blocker].append(transaction_id)
                self.transaction_list.get(transaction_id).status = TransactionStatus.READ
                self.transaction_list.get(transaction_id).query_buffer = [variable_id]
            elif read_only and read_result[1] == -2:
                self.abort(transaction_id, time)
            else:
                blockers = read_result[1]
                if blockers[0] != -1:
                    self.data_wait_table[variable_id].append(transaction_id)
                for blocker in blockers:
                    self.transaction_wait_table[transaction_id].add(blocker)
                    self.block_table[blocker].append(transaction_id)
                self.transaction_list.get(transaction_id).status = TransactionStatus.READ
                self.transaction_list.get(transaction_id).query_buffer = [variable_id]

    def write(self, transaction_id, variable_id, value):
        """
        Write the variable for a given transaction. This internally calls Data Managers write.
        :param transaction_id: ID of the transaction.
        :param variable_id: ID of the variable.
        :param value: New value of the variable to be written.
        :return: None
        """
        LOGGER.info("T" + str(transaction_id) + " requested to write " + str(variable_id) + " as " + str(value))
        LOGGER.debug("Calling the Data Manager to write " + str(variable_id))
        write_result = self.DM.write(transaction_id, variable_id, self.block_table)
        if write_result[0]:
            LOGGER.info("Write successful. Written to site(s): " + write_result[1].__str__())
            sites_touched = set(write_result[1])
            self.transaction_list.get(transaction_id).touch_set = sites_touched
            self.transaction_list.get(transaction_id).set_commit_list(variable_id, (value, set(write_result[1])))
            self.transaction_list.get(transaction_id).status = TransactionStatus.NORMAL
            self.transaction_list.get(transaction_id).set_lock_list(variable_id, 'w')
            if transaction_id in self.transaction_wait_table:
                del self.transaction_wait_table[transaction_id]
        else:
            LOGGER.info("Write unsuccessful. Blocked by site(s): " + write_result[1].__str__())
            self.data_wait_table[variable_id].append(transaction_id)
            blockers = write_result[1]
            for blocker in blockers:
                self.transaction_wait_table[transaction_id].add(blocker)
                self.block_table[blocker].append(transaction_id)
            self.transaction_list[transaction_id].status = TransactionStatus.WRITE
            self.transaction_list[transaction_id].query_buffer = [variable_id, value]

    def fail(self, site_id, time):
        """
        Fail the site corresponding to the given ID. This internally calls Data Managers Fail.
        :param site_id: ID of the site.
        :param time: Current time of the system.
        :return: None
        """
        LOGGER.info("site " + str(site_id) + " failing")
        LOGGER.debug("Calling Data Manager to fail site " + str(site_id))
        self.DM.fail(site_id)
        self.fail_history[site_id].append(time)
        for transaction_id in self.transaction_list:
            if site_id in self.transaction_list.get(transaction_id).touch_set:
                self.transaction_list.get(transaction_id).abort = AbortStatus.TRUE

    def recover(self, site_id):
        """
        Recover the site corresponding to the given ID. This internally calls Data Managers Recover.
        :param site_id: ID of the site.
        :return: None
        """
        LOGGER.info("site " + str(site_id) + " recovering")
        LOGGER.debug("Calling Data Manager to fail site " + str(site_id))
        self.DM.recover(site_id)

    def end(self, transaction_id, time):
        """
        End the transaction. This internally calls Data Managers end.
        :param transaction_id: ID of the transaction.
        :param time: Current time of the system.
        :return: None
        """
        LOGGER.info("end T" + str(transaction_id))
        trans = self.transaction_list.get(transaction_id)
        sites_touched = trans.touch_set
        start_time = trans.start_time
        end_time = time
        if self.transaction_list[transaction_id].abort == AbortStatus.TRUE:
            LOGGER.debug("Aborting transaction: " + str(transaction_id))
            self.abort(transaction_id, time)
        else:
            LOGGER.debug("Committing transaction: " + str(transaction_id))
            self.commit(transaction_id, time)

    def dump(self, site=None, variable=None):
        """
        Take a snapshot of the Sites and print to stdout.
        :param site: Site to be printed (optional)
        :param variable: Variable to be printed (optional)
        :return: None
        """
        if site is None and variable is None:
            LOGGER.info("dump all data")
        elif site is None:
            LOGGER.info("dump data x" + str(variable) + " from all site")
        else:
            LOGGER.info("dump data on site " + str(site))

        LOGGER.debug("Calling Data Managers dump().")
        self.DM.dump(site, variable)

    def deadlock_detection(self, time):
        """
        Detect deadlock in the system. If found, abort the youngest transaction.
        :param time: Current time of the system.
        :return: None
        """
        LOGGER.debug("Detecting deadlock @ tick " + str(time))
        visited = collections.defaultdict(int)
        for t in self.transaction_list:
            visited[t] = 0
        for t in visited:
            if not visited.get(t):
                stack = [t]
                while len(stack) != 0:
                    f = stack[-1]
                    if not visited.get(f) and f in self.transaction_wait_table:
                        visited[f] = 1
                        ghost_transaction_list = []
                        for c in self.transaction_wait_table[f]:
                            if c != -1 and c not in self.transaction_list:
                                ghost_transaction_list.append(c)
                        for ghost_transaction in ghost_transaction_list:
                            self.transaction_wait_table[f].remove(ghost_transaction)
                        for c in self.transaction_wait_table[f]:
                            if c == -1:
                                continue
                            if visited.get(c, 0) == 1:
                                LOGGER.error("Deadlock detected.")
                                cur = c
                                youngest_transaction = f
                                while cur != f:
                                    if self.transaction_list[cur].start_time > \
                                            self.transaction_list[youngest_transaction].start_time:
                                        youngest_transaction = cur
                                    for next_trans in self.transaction_wait_table[cur]:
                                        if visited[next_trans] == 1:
                                            cur = next_trans
                                LOGGER.info(
                                    "Youngest transaction found. Aborting transaction " + str(youngest_transaction))
                                self.abort(youngest_transaction, time)
                            elif visited[c] == 0:
                                stack.append(c)
                    else:
                        visited[f] = 2
                        stack.pop()

    def print_status(self):
        """
        Print Status of a transaction.
        :return: None
        """
        print("transaction_wait_table : ", self.transaction_wait_table.__str__())
        print("block_table            : ", self.block_table.__str__())
        print("data_wait_table        : ", self.data_wait_table.__str__())
        for t_id in self.transaction_list:
            print(self.transaction_list[t_id].ID)

    def abort(self, transaction_id, time):
        """
        Abort a given transaction. This internally calls Data Managers Abort.
        :param transaction_id: ID of the transaction.
        :param time: Current time of the system.
        :return: None.
        """
        LOGGER.info("Abort transaction " + str(transaction_id))
        LOGGER.debug("Releasing Locks.")
        self.release_locks(transaction_id, time)
        del self.transaction_list[transaction_id]
        if transaction_id in self.transaction_wait_table:
            del self.transaction_wait_table[transaction_id]
        if transaction_id in self.block_table:
            del self.block_table[transaction_id]
        for data in self.data_wait_table:
            for i, t_id in enumerate(self.data_wait_table[data]):
                if t_id == transaction_id:
                    del self.data_wait_table[data][i]
        self.final_result[transaction_id] = "abort"

    def commit(self, transaction_id, time):
        """
        Commit a given transaction. This internally calls Data Managers Commit.
        :param transaction_id: ID of the transaction.
        :param time: Current time of the system.
        :return: None.
        """
        LOGGER.info("Commit transaction " + str(transaction_id))
        trans = self.transaction_list[transaction_id]
        LOGGER.debug("Calling Data Managers commit.")
        self.DM.commit(trans.commit_list)
        LOGGER.debug("Releasing Locks.")
        self.release_locks(transaction_id, time)
        del self.transaction_list[transaction_id]
        if transaction_id in self.transaction_wait_table:
            del self.transaction_wait_table[transaction_id]
        if transaction_id in self.block_table:
            del self.block_table[transaction_id]
        for var in trans.commit_list:
            self.commit_summary[var] = trans.commit_list.get(var)
        self.final_result[transaction_id] = "commit"

    def release_locks(self, transaction_id, time):
        """
        Release locks held by a transaction. This is usually used after abort/commit.
        This internally calls Data Managers release_locks.
        :param transaction_id: ID of the transaction.
        :param time: Current time of the system.
        :return: None.
        """
        LOGGER.info("Release lock held by T" + str(transaction_id))
        locks = self.transaction_list[transaction_id].lock_list
        LOGGER.debug("Calling Data Managers release lock.")
        free_datas = self.DM.release_locks(transaction_id, locks)
        msg = "newly freed data:"
        for fd in free_datas:
            msg += " " + str(fd)
        LOGGER.debug(msg)
        retry_list = []
        for free_data in free_datas:
            if free_data in self.data_wait_table:
                for tid in self.data_wait_table[free_data]:
                    if tid not in retry_list:
                        retry_list.append(tid)
        for free_data in free_datas:
            if free_data in self.data_wait_table:
                del self.data_wait_table[free_data]
        for tid in retry_list:
            LOGGER.debug("Calling retry after releasing locks.")
            self.retry(tid, time)

    def retry(self, transaction_id, time):
        """
        Retry a transaction.
        :param transaction_id: ID of the transaction.
        :param time: Current time of the system.
        :return: None.
        """
        LOGGER.info("Retrying transaction: " + str(transaction_id))
        trans = self.transaction_list[transaction_id]
        if self.transaction_list[transaction_id].status == TransactionStatus.READ:
            LOGGER.info("Retrying read transaction: " + str(transaction_id))
            self.read(transaction_id, trans.query_buffer[0], time)
        elif self.transaction_list[transaction_id].status == TransactionStatus.WRITE:
            LOGGER.info("Retrying write transaction: " + str(transaction_id))
            self.write(transaction_id, trans.query_buffer[0], trans.query_buffer[1])

    def print_final_status(self):
        """
        Print summary after processing the input file.
        :return: None.
        """
        print("\n[summary]")
        for transaction_id in self.final_result:
            print("T" + str(transaction_id) + " :", self.final_result[transaction_id])
        for var in self.commit_summary:
            print(var, "final value: ", self.commit_summary[var])
