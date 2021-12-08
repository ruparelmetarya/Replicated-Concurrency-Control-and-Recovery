"""
Author: Safwan Mahmood
email: sm9453@nyu.edu
Created On: 11/27/2021
"""
import collections

from DataManager.site import Site
from Enums.lock_type import LockType
from Logger.logger import Logger
from TransactionManager.transaction import Transaction

LOGGER = Logger.get_logger(__name__)


class DataManager:
    """
    Represents a cluster of Sites present in the System.
    """

    def __init__(self):
        self.sites_db = collections.defaultdict(Site)
        self.var_sites = collections.defaultdict(list)
        self._init_sites()
        self._init_var_sites()

    def _init_sites(self):
        """
        Initialize sites 1-10.
        :return: None
        """
        for i in range(1, 11):
            self.sites_db[i] = Site(i)
        LOGGER.info("Initialized Sites")

    def _init_var_sites(self):
        """
        Initialize variable x1-20.
        :return: None
        """
        for i in range(1, 21):
            var = "x" + str(i)
            # even index replicated in all sites
            if i % 2 == 0:
                self.var_sites[var] = [self.sites_db[j] for j in range(1, 11)]
            else:
                # Odd index not replicated
                self.var_sites[var] = [self.sites_db[1 + (i % 10)]]
        LOGGER.info("Initialized variable on respective sites")

    def generate_cache_for_ro(self, trans: Transaction):
        """
        Maintain cache for read-only transactions, aids readonly multi-version read protocol.
        :param trans: Transaction to cache
        :return: None
        """
        for i, site in self.sites_db.items():
            if site.is_running:
                vars = site.variables
                for var in vars:
                    if site.is_valid_variable(var):
                        data = vars[var].value
                        trans.set_cache(var, data)
                        LOGGER.info("Cache updated with {var}:{data}".format(var=var, data=data))
        LOGGER.info("Cache generated for transaction {t}".format(t=trans.ID))

    def read(self, trans: Transaction, variable_id):
        """
        Reads variable from db, checks for lock, gives the sites read from.
        If the transaction can't get the lock, gives list of transactions it is waiting on.
        :param trans: Read transaction
        :param variable_id: Variable to read
        :return: Bool, list of sites or waiting on
        """
        # check for readonly transaction
        if trans.read_only:
            # if found in cache
            if trans.cache and variable_id in trans.cache:
                LOGGER.info(
                    "Transaction ID: {id} of read only type read {vars} : {value}".format(id=trans.ID, vars=variable_id,
                                                                                          value=trans.cache[
                                                                                              variable_id]))
                return True, []
            elif trans.cache and variable_id not in trans.cache:
                LOGGER.debug("Variable {var} not found in cache and not first time".format(var=variable_id))
                return False, [-2]
            else:
                # first time read, generate cache
                self.generate_cache_for_ro(trans)
                if variable_id in trans.cache:
                    LOGGER.info("Transaction ID: {id} of read only type read {vars} : {value}".format(
                        id=trans.ID,
                        vars=variable_id,
                        value=trans.cache[
                            variable_id])
                    )
                    return True, []
                else:
                    # unexpected cache miss, wait or retry.
                    trans.cache.clear()
                    LOGGER.debug("Cache miss")
                    return False, [-1]
        else:
            # other read transactions
            sites = self.var_sites.get(variable_id)
            for site in sites:
                if site.is_running and site.is_valid_variable(
                        variable_id):
                    # check for running sites and valid variable in that site
                    locked_by = site.lock_table[
                        variable_id].locks
                    # get transactions that have lock on variable on this site
                    if site.get_lock_type(variable_id) == LockType.WRITE:
                        # if write lock
                        if variable_id in locked_by:
                            # if current transaction has lock available for the variable
                            LOGGER.info(
                                "Transaction ID: {id} which had lock already available read {vars} : {value}".format(
                                    id=trans.ID, vars=variable_id, value=site.variables.get(variable_id).value))
                            return True, [site.site_num]
                        else:
                            # Lock wasn't acquired, return the list of blocking/waiting on transactions
                            LOGGER.debug("Couldn't acquire read lock")
                            return False, locked_by
                    else:
                        # Got read lock, acquire it and return the site at which it was acquired.
                        site.add_variable_lock(variable_id, trans.ID, LockType.READ)
                        LOGGER.info("Transaction ID: {id} read {vars} : {value}".format(id=trans.ID, vars=variable_id,
                                                                                        value=site.variables.get(
                                                                                            variable_id).value))
                        return True, [site.site_num]
            # didn't find read lock or site
            return False, [-1]

    def write(self, transaction_id, variable_id, block_table):
        """
        Writes the variable and value to the db in the sites of the variable. If acquires the exclusive write
        lock, returns the sites updated. If lock not acquired, returns the transactions blocking the current
        write.
        :param transaction_id: ID of a Transaction.
        :param variable_id: ID of the Variable to write.
        :param block_table: Block table.
        :return: tuple(Bool, sites or waiting on list).
        """
        sites = self.var_sites[variable_id]
        blocked_by = set()
        sites_updated = []
        write_success = True
        for site in sites:
            if site.is_running:
                sites_updated.append(site.site_num)
                # get transactions that hold lock on the variable
                locked_by = site.lock_table[variable_id].locks
                if site.get_lock_type(variable_id) == LockType.WRITE:
                    LOGGER.debug("Write type lock")
                    if variable_id in locked_by:
                        write_success = True
                        LOGGER.debug("Current transaction has the write lock. Trans:" + str(transaction_id))
                    else:
                        LOGGER.debug("Couldn't get the write lock. Trans:" + str(transaction_id))
                        return False, locked_by
                elif site.get_lock_type(variable_id) == LockType.READ:
                    if locked_by[0] == transaction_id and len(
                            locked_by) == 1:
                        if transaction_id not in block_table:
                            LOGGER.debug("Promoted the read lock")
                            continue
                        else:
                            LOGGER.debug("Can't promote read lock and waiting dependency")
                            return False, block_table.get(transaction_id)
                    else:
                        write_success = False
                        for tran in locked_by:
                            if tran != transaction_id:
                                blocked_by.add(tran)
        # Found write lock, return updated sites
        if write_success:
            for site in sites:
                site.add_variable_lock(variable_id, transaction_id, LockType.WRITE)
                LOGGER.debug("Write locked on {var} by {t}".format(var=variable_id, t=transaction_id))
                return True, sites_updated
        elif len(sites_updated) == 0:
            LOGGER.debug("No site found")
            return False, [-1]
        else:
            blocked_by = list(blocked_by)
            LOGGER.debug("No write lock found, have to wait")
            return False, blocked_by

    def write_val_to_database(self, ID, val, sites_touched):
        """
        Update the value in the sites.
        :param ID: ID of the Variable
        :param val: values
        :param sites_touched: Sites to updated
        :return: None
        """
        for site in sites_touched:
            self.sites_db.get(site).write_to_variable(ID, val)
            LOGGER.info("Variable: {id} = {value} written in {site}".format(id=ID, value=val, site=site))

    def commit(self, commit_list):
        """
        Commit values to db.
        :param commit_list: Commit list
        :return: None
        """
        if len(commit_list) != 0:
            for ID, (val, sites_touched) in commit_list.items():
                self.write_val_to_database(ID, val, sites_touched)
                LOGGER.debug("Write to DB successful.")

    def release_locks(self, transaction_id, lock_dict):
        """
        Release lock from a transaction.
        :param transaction_id: Transaction
        :param lock_dict: locks
        :return: set
        """
        all_freed = []
        for varID in lock_dict:
            sites = self.var_sites[varID]
            locked = False
            for site in sites:
                if site.is_running:
                    site.remove_variable_lock(varID, transaction_id)
                    if not site.is_variable_free(varID):
                        locked = True
            if not locked:
                all_freed.append(varID)
                LOGGER.debug("{var} is freed".format(var=varID))
        return set(all_freed)

    def fail(self, site_id):
        """
        Fail a site.
        :param site_id: ID of the Site to fail
        :return: None
        """
        site = self.sites_db[site_id]
        if site.is_running:
            LOGGER.info("Failed site: {site}".format(site=site.site_num))
            site.fail_site()

    def recover(self, site_id):
        """
        Recover site.
        :param site_id: Id of the Site to recover
        :return: None
        """
        site = self.sites_db[site_id]
        if not site.is_running:
            LOGGER.info("Recovered site: {site}".format(site=site.site_num))
            site.recover_site()

    def dump(self, site_id=None, variable_id=None):
        """
        Prints the status of the DB.
        :param site_id: Site to dump
        :param variable_id: Variable to dump
        :return: None
        """
        # DB status for sites and variables
        if not site_id and not variable_id:
            for i, site in self.sites_db.items():
                if site.is_running:
                    print("Site ID: " + str(site.site_num))
                    vars = site.variables
                    site_vars = []
                    for var in vars:
                        site_vars.append((var, vars[var].value))
                    print(site_vars)
        elif not variable_id:
            site = self.sites_db[site_id]
            if site.is_running:
                print("Site ID: " + str(site.site_num))
                vars = site.variables
                site_vars = []
                for var in vars:
                    site_vars.append((var, vars[var].value))
                print(site_vars)

        elif not site_id:
            sites = self.var_sites[variable_id]
            sites_var = []
            for site in sites:
                print("Site ID: " + str(site.site_num))
                if site.is_running:
                    var = site.variable(variable_id)
                    sites_var.append((var.id, var.value))
                print(sites_var)
