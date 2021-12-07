"""
Author: Metarya Ruparel
email: msr9732@nyu.edu
Created On: 11/26/2021
"""
import collections
from DataManager.data_manager import DataManager


class TransactionManager:
    def __init__(self):
        self.DM = DataManager()
        self.transaction_list = collections.defaultdict()
        self.transaction_wait_table = collections.defaultdict()
        self.data_wait_table = collections.defaultdict()
        self.block_table = collections.defaultdict()
        self.fail_history = collections.defaultdict()
        self.final_result = collections.defaultdict()
        self.commit_summary = collections.defaultdict()

if __name__ == '__main__':
    print('Heyy')

