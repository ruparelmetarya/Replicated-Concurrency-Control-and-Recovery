"""
Author: Metarya Ruparel
email: msr9732@nyu.edu
Created On: 11/26/2021
"""

import collections
import re

from DataManager.data_manager import DataManager
from Enums.transaction_status import TransactionStatus
from Enums.abort_status import AbortStatus
from TransactionManager.transaction import Transaction


class TransactionManager:
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
        infile = open(filename, 'r')
        return infile.readlines()

    def parser(self, filename):
        data = self.read_file(filename=filename)
        line_num = time = 0
        for line in data:
            line_num += 1
            time += 1
            print("\n" + str(time) + ">>>")
            self.deadlock_detection(time)
            self.resurrect(time)
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

            if operation_name == "begin":
                if len(operation_arg) != 1:
                    errmsg = "error: operation [begin] requires exactly 1 argument, " + str(len(operation_arg))
                    errmsg += " provided in line " + str(line_num)
                    raise ValueError(errmsg)
                transaction_id = int(operation_arg[0].strip()[1:])
                self.begin(transaction_id, time)

            elif operation_name == "beginRO":
                if len(operation_arg) != 1:
                    errmsg = "error: operation [begin] requires exactly 1 argument, " + str(len(operation_arg))
                    errmsg += " provided in line " + str(line_num)
                    raise ValueError(errmsg)
                transaction_id = int(operation_arg[0].strip()[1:])
                self.begin(transaction_id, time, read_only=True)

            elif operation_name == "R":
                if len(operation_arg) != 2:
                    errmsg = "error: operation [R] requires exactly 2 argument, " + str(len(operation_arg))
                    errmsg += " provided in line " + str(line_num)
                transaction_id = int(operation_arg[0].strip()[1:])
                variable_id = operation_arg[1].strip()
                self.read(transaction_id, variable_id, time)

            elif operation_name == "W":
                if len(operation_arg) != 3:
                    errmsg = "error: operation [W] requires exactly 3 argument, " + str(len(operation_arg))
                    errmsg += " provided in line " + str(line_num)
                    raise ValueError(errmsg)
                transaction_id = int(operation_arg[0].strip()[1:])
                variable_id = operation_arg[1].strip()
                value = int(operation_arg[2].strip())
                self.write(transaction_id, variable_id, value)

            elif operation_name == "dump":
                print("operation_arg: ", operation_arg)
                if len(operation_arg) != 1:
                    errmsg = "error: operation [dump] requires no more than 1 argument, " + str(len(operation_arg))
                    errmsg += " provided in line " + str(line_num)
                    raise ValueError(errmsg)
                if len(operation_arg[0]) == 0:
                    self.dump()
                elif operation_arg[0][0] == 'x':
                    variable_id = operation_arg[0].strip()
                    self.dump(variable=variable_id)
                else:
                    site_id = int(operation_arg[0].strip())
                    self.dump(site=site_id)

            elif operation_name == "end":
                if len(operation_arg) != 1:
                    errmsg = "error: operation [end] requires exactly 1 argument, " + str(len(operation_arg))
                    errmsg += " provided in line " + str(line_num)
                    raise ValueError(errmsg)
                transaction_id = int(operation_arg[0].strip()[1:])
                self.end(transaction_id, time)

            elif operation_name == "fail":
                if len(operation_arg) != 1:
                    errmsg = "error: operation [fail] requires exactly 1 argument, " + str(len(operation_arg))
                    errmsg += " provided in line " + str(line_num)
                    raise ValueError(errmsg)
                site_id = int(operation_arg[0].strip())
                self.fail(site_id, time)

            elif operation_name == "recover":
                if len(operation_arg) != 1:
                    errmsg = "error: operation [fail] requires exactly 1 argument, " + str(len(operation_arg))
                    errmsg += " provided in line " + str(line_num)
                    raise ValueError(errmsg)
                site_id = int(operation_arg[0].strip())
                self.recover(site_id)

            else:
                errmsg = "error: can not recognize operation name: [" + operation_name
                errmsg += "] in line " + str(line_num)
                raise ValueError(errmsg)

            self.print_status()

    def begin(self, transaction_id, time, read_only=False):
        msg = "begin T" + str(transaction_id)
        if read_only:
            msg += "(read-only)"
        msg += " @ tick " + str(time)
        print(msg)
        t = Transaction(_id=transaction_id, _start_time=time, _read_only=read_only)
        self.transaction_list[transaction_id] = t

    def read(self, transaction_id, variable_id, time):
        msg = "T" + str(transaction_id) + " attempt to read " + str(variable_id)
        print(msg)
        read_only = self.transaction_list.get(transaction_id).read_only
        read_result = self.DM.read(self.transaction_list.get(transaction_id), variable_id)
        if read_result[0]:
            if not read_only:
                sites_touched = set(read_result[1])
                self.transaction_list.get(transaction_id).touch_set = sites_touched
                self.transaction_list.get(transaction_id).status = TransactionStatus.NORMAL
                # variable is already locked by transaction
                if self.transaction_list.get(transaction_id).lock_list.get(variable_id, None) is None:
                    self.transaction_list.get(transaction_id).set_lock_list(variable_id, 'r')
                if self.transaction_wait_table.get(transaction_id, None) is not None:
                    del self.transaction_wait_table[transaction_id]
        # blocked
        else:
            # cache failed:
            if read_only and read_result[1] == -1:
                blocker = -1
                self.transaction_wait_table[transaction_id].add(blocker)
                self.block_table[blocker].append(transaction_id)
                self.transaction_list.get(transaction_id).status = TransactionStatus.READ
                self.transaction_list.get(transaction_id).query_buffer = [variable_id]
            # data not in cache:
            elif read_only and read_result[1] == -2:
                self.abort(transaction_id)
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
        msg = "T" + str(transaction_id) + " attempt to write " + str(variable_id) + " as " + str(value)
        print(msg)
        write_result = self.DM.write(transaction_id, variable_id)
        print("write_result: ", write_result)
        if write_result[0]:
            sites_touched = set(write_result[1])
            self.transaction_list.get(transaction_id).touch_set = sites_touched
            self.transaction_list.get(transaction_id).set_commit_list(variable_id, value)
            self.transaction_list.get(transaction_id).status = TransactionStatus.NORMAL
            self.transaction_list.get(transaction_id).set_lock_list(variable_id, 'w')
            if transaction_id in self.transaction_wait_table:
                del self.transaction_wait_table[transaction_id]
        else:
            self.data_wait_table[variable_id].append(transaction_id)
            blockers = write_result[1]
            for blocker in blockers:
                self.transaction_wait_table[transaction_id].add(blocker)
                self.block_table[blocker].append(transaction_id)
            self.transaction_list[transaction_id].status = TransactionStatus.WRITE
            self.transaction_list[transaction_id].query_buffer = [variable_id, value]

    def fail(self, site_id, time):
        msg = "site " + str(site_id) + " failed"
        print(msg)
        self.DM.fail(site_id)
        self.fail_history[site_id].append(time)
        for transaction_id in self.transaction_list:
            if site_id in self.transaction_list.get(transaction_id).touch_set:
                self.transaction_list.get(transaction_id).abort = AbortStatus.TRUE

    def recover(self, site_id):
        msg = "recover site " + str(site_id)
        print(msg)
        self.DM.recover(site_id)

    def end(self, transaction_id, time):
        msg = "end T" + str(transaction_id)
        print(msg)
        trans = self.transaction_list.get(transaction_id)
        sites_touched = trans.touch_set
        start_time = trans.start_time
        end_time = time
        if self.transaction_list[transaction_id].abort == AbortStatus.TRUE:
            self.abort(transaction_id, time)
        else:
            self.commit(transaction_id, time)

    def dump(self, site=None, variable=None):
        print("TM phase: ")
        if site is None and variable is None:
            msg = "dump all data"
            print(msg)
        elif site is None:
            msg = "dump data x" + str(variable) + " from all site"
            print(msg)
        else:
            msg = "dump data on site " + str(site)
            print(msg)
        self.DM.dump(site, variable)

    # check if a transaction should be aborted due to site failure
    def validation(self, sites_touched, start_time, end_time):
        for site in sites_touched:
            if site in self.fail_history:
                for fail_time in self.fail_history[site]:
                    if start_time < fail_time < end_time:
                        return False
        return True

    def deadlock_detection(self, time):
        msg = "detecting deadlock @ tick " + str(time)
        print(msg)
        # 0: not visited    1: visiting     2:finished
        visited = collections.defaultdict(int)
        for t in self.transaction_list:
            visited[t] = 0
        for t in visited:
            if not visited.get(t):
                stack = [t]
                # visited[t] = 1
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
                                print("There's a circle. Let the killing begin")
                                cur = c
                                youngest_transaction = f
                                while cur != f:
                                    if self.transaction_list[cur].start_time > self.transaction_list[
                                            youngest_transaction].start_time:
                                        youngest_transaction = cur
                                    for next_trans in self.transaction_wait_table[cur]:
                                        if visited[next_trans] == 1:
                                            cur = next_trans
                                print("Prey located, let's sacrifice transaction " + str(youngest_transaction))
                                self.abort(youngest_transaction, time)
                            elif visited[c] == 0:
                                stack.append(c)
                    else:
                        visited[f] = 2
                        stack.pop()

    def resurrect(self, time):
        msg = "resurrect transactions blocked by failed site"
        print(msg)
        if -1 in self.block_table:
            for i, trans_id in enumerate(self.block_table[-1]):
                if self.transaction_list[trans_id].status == TransactionStatus.READ:
                    variable_id = self.transaction_list[trans_id].query_buffer[0]
                    del self.block_table[-1][i]
                    self.read(trans_id, variable_id, time)
                else:
                    variable_id = self.transaction_list[trans_id].query_buffer[0]
                    value = self.transaction_list[trans_id].query_buffer[0]
                    del self.block_table[-1][i]
                    self.write(trans_id, variable_id, value)

    def print_status(self):
        print("transaction_wait_table : ", self.transaction_wait_table.__str__())
        print("block_table            : ", self.block_table.__str__())
        print("data_wait_table        : ", self.data_wait_table.__str__())
        for t_id in self.transaction_list:
            print(self.transaction_list[t_id].ID)

    def abort(self, transaction_id, time):
        msg = "abort transaction " + str(transaction_id)
        print(msg)
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
        msg = "commit transaction " + str(transaction_id)
        print(msg)
        trans = self.transaction_list[transaction_id]
        self.DM.commit(trans.commit_list)
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
        msg = "release lock hold by T" + str(transaction_id) + " and give them to other blocked transactions"
        print(msg)
        locks = self.transaction_list[transaction_id].lock_list
        free_datas = self.DM.release_locks(transaction_id, locks)
        msg = "newly freed data:"
        for fd in free_datas:
            msg += " " + str(fd)
        print(msg)
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
            self.retry(tid, time)

    def retry(self, transaction_id, time):
        trans = self.transaction_list[transaction_id]
        if self.transaction_list[transaction_id].status == TransactionStatus.READ:
            self.read(transaction_id, trans.query_buffer[0], time)
        elif self.transaction_list[transaction_id].status == TransactionStatus.WRITE:
            self.write(transaction_id, trans.query_buffer[0], trans.query_buffer[1])

    def print_final_status(self):
        print("\n[summary]")
        for transaction_id in self.final_result:
            print("T" + str(transaction_id) + " :", self.final_result[transaction_id])
        for var in self.commit_summary:
            print(var, "final value: ", self.commit_summary[var])
