"""
Author: Safwan Mahmood
email: sm9453@nyu.edu
Created On: 11/27/2021
"""
import collections

from DataManager.lock import Lock
from DataManager.site import Site
from Enums.lock_type import LockType
from TransactionManager.transaction import Transaction

from Logger.logger import Logger
logger = Logger.get_logger(__name__)


class DataManager:
    def __init__(self):
        self.sites_db = collections.defaultdict(Site)  ##site storage {1:Site(i)}
        self.var_sites = collections.defaultdict(list) ##sites in which variable is present {"x1":[Site(i).....]}
        self._init_sites()
        self._init_var_sites()


    def _init_sites(self):
        """
        Initialize sites 1-10
        :return: None
        """
        for i in range(1, 11):
            self.sites_db[i] = Site(i)
        logger.info("Initialized Sites")

    def _init_var_sites(self):
        """
        Initialize variable x1-20
        :return: None
        """
        for i in range(1, 21):
            var = "x" + str(i)
            if i % 2 == 0: ##even index replicated in all sites
                self.var_sites[var] = [self.sites_db[j] for j in range(1, 11)]
            else: ##Odd index not replicated
                self.var_sites[var] = [self.sites_db[1 + (i % 10)]]
        logger.info("Initialized variable on respective sites")

    def generate_cache_for_ro(self, trans: Transaction) -> None:
        """
        Maintain cache for read-only transactions, aids readonly multi-version read protocol
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
                        logger.info("Cache updated with {var}:{data}".format(var=var,data=data))
        logger.info("Cache generated for transaction {t}".format(t = trans.ID))


    def read(self, trans: Transaction, variable_id) -> (bool, list):
        """
        Reads variable from db, checks for lock, gives the sites read from.
        If the transaction can't get the lock, gives list of transactions it is waiting on.
        :param trans: Read transaction
        :param variable_id: Variable to read
        :return: Bool, list of sites or waiting on
        """
        if trans.read_only: ##check for readonly transaction
            if trans.cache and variable_id in trans.cache: ##if found in cache
                logger.info("Transaction ID: {id} of read only type read {vars} : {value}".format(id=trans.ID,vars=variable_id,value=trans.cache[variable_id]))
                return True, []
            elif trans.cache and variable_id not in trans.cache: ##abort since it is not the first read and variable is not present in cache
                logger.debug("Variable {var} not found in cache and not first time".format(var=variable_id))
                return False, [-2]
            else: ##first time read, generate cache
                self.generate_cache_for_ro(trans)
                if variable_id in trans.cache: ##get value from cache
                    logger.info("Transaction ID: {id} of read only type read {vars} : {value}".format(id=trans.ID,vars=variable_id,value=trans.cache[variable_id]))
                    return True, []
                else: ##unexpected cache miss, wait or retry.
                    trans.cache.clear()
                    logger.debug("Cache miss")
                    return False, [-1]
        else: ##other read transactions
            sites = self.var_sites.get(variable_id) ##get all sites where variable is present
            for site in sites:
                if site.is_running and site.is_valid_variable(variable_id): ##check for running sites and valid variable in that site
                    locked_by = site.lock_table[variable_id].locks ##get transactions that have lock on variable on this site
                    if site.get_lock_type(variable_id) == LockType.WRITE: ##if write lock
                        if variable_id in locked_by: ##if current transaction has lock available for the variable
                            logger.info("Transaction ID: {id} which had lock already available read {vars} : {value}".format(id=trans.ID,vars=variable_id,value=site.variables.get(variable_id).value))
                            return True, [site.site_num]
                        else: ##Lock wasn't acquired, return the list of blocking/waiting on transactions
                            logger.debug("Couldn't acquire read lock")
                            return False, locked_by
                    else:   ##Got read lock, acquire it and return the site at which it was acquired.
                        site.add_variable_lock(variable_id, trans.ID, LockType.READ)
                        logger.info("Transaction ID: {id} read {vars} : {value}".format(id=trans.ID, vars=variable_id, value= site.variables.get(variable_id).value))
                        return True, [site.site_num]
            ##didn't find read lock or site
            return False, [-1]


    def write(self, transaction_id, variable_id, block_table):
        """
        Writes the variable and value to the db in the sites of the variable
        If acquires the exclusive write lock, returns the sites updated
        If lock not acquired, returns the transactions blocking the current write.
        :param transaction_id: Write Transaction
        :param variable_id: Variable to write
        :param block_table: Block table
        :return: Bool, sites or waiting on list
        """
        sites = self.var_sites[variable_id] ##sites of the variable in the db
        blocked_by = set() ##waiting on set
        sites_updated = [] ##sites updated by the transaction
        write_success = True ##write went through
        for site in sites:
            if site.is_running:
                sites_updated.append(site.site_num)
                locked_by = site.lock_table[variable_id].locks ##get transactions that hold lock on the variable
                if site.get_lock_type(variable_id) == LockType.WRITE: ##If lock is write type
                    logger.debug("Write type lock")
                    if variable_id in locked_by: ##If current transaction has the lock already
                        write_success = True
                        logger.debug("Current transaction has the write lock. Trans:" + str(transaction_id))
                    else: ##return waiting on list
                        logger.debug("Couldn't get the write lock. Trans:" + str(transaction_id))
                        return False, locked_by
                elif site.get_lock_type(variable_id) == LockType.READ: ##if lock is read type
                    if locked_by[0] == transaction_id and len(locked_by) == 1: ##if current transaction has the read lock
                        print(block_table)
                        if transaction_id not in block_table: ##no transaction waiting on it.
                            logger.debug("Promoted the read lock")
                            continue
                        else: ##transaction waiting on it/can't promote the lock
                            logger.debug("Can't promote read lock and waiting dependency")
                            return False, block_table.get(transaction_id)
                    else: ##add the waiting on transactions
                        write_success = False
                        for tran in locked_by:
                            if tran != transaction_id:
                                blocked_by.add(tran)
        if write_success: ##Found write lock, return updated sites
            for site in sites:
                site.add_variable_lock(variable_id, transaction_id, LockType.WRITE)
                logger.debug("Write locked on {var} by {t}".format(var=variable_id,t=transaction_id))
                return True, sites_updated
        elif len(sites_updated) == 0: ##no site found
            logger.debug("No site found")
            return False, [-1]
        else: ##return the transactions the current transaction is waiting on for write lock
            blocked_by = list(blocked_by)
            logger.debug("No write lock found, have to wait")
            return False, blocked_by

    def write_val_to_database(self, ID, val, sites_touched):
        """
        Update the value in the sites
        :param ID: Variable
        :param val: values
        :param sites_touched: Sites to updated
        :return:
        """
        for site in sites_touched:
            self.sites_db.get(site).write_to_variable(ID, val) ##update value
            logger.info("Variable: {id} = {value} written in {site}".format(id=ID, value=val,site=site))

    def commit(self, commit_list):
        """
        Commit values to db
        :param commit_list: Commit list
        :return:
        """
        if len(commit_list) != 0:
            for ID, (val, sites_touched) in commit_list.items():
                self.write_val_to_database(ID, val, sites_touched)
                logger.debug("Write to DB went through")

    def release_locks(self, transaction_id, lock_dict):
        """
        Release lock from a transaction
        :param transaction_id: Transaction
        :param lock_dict: locks
        :return:
        """
        all_freed = []
        for varID in lock_dict:
            sites = self.var_sites[varID]
            locked = False
            for site in sites: ##remove locks of variables from site
                if site.is_running:
                    site.remove_variable_lock(varID, transaction_id)
                    if not site.is_variable_free(varID):
                        locked = True
            if not locked: ##add to free list if freed
                all_freed.append(varID)
                logger.debug("{var} is freed".format(var=varID))
        return set(all_freed)

    def fail(self, site_id):
        """
        Fail a site
        :param site_id: Site to fail
        :return:
        """
        site = self.sites_db[site_id]
        if site.is_running:
            logger.info("Failed site: {site}".format(site=site.site_num))
            site.fail_site()

    def recover(self, site_id):
        """
        Recover site
        :param site_id: Site to recover
        :return:
        """
        site = self.sites_db[site_id]
        if not site.is_running:
            logger.info("Recovered site: {site}".format(site=site.site_num))
            site.recover_site()


    def dump(self, site_id=None, variable_id=None):
        """
        Prints the status of the DB
        :param site_id: Site to dump
        :param variable_id: Variable to dump
        :return:
        """
        ##DB status for sites and variables
        if not site_id and not variable_id: ##All sites and variables
            for i, site in self.sites_db.items():
                if site.is_running:
                    print("Site ID: " + str(site.site_num))
                    vars = site.variables
                    site_vars = []
                    for var in vars:
                        site_vars.append((var,vars[var].value))
                    print(site_vars)
        elif not variable_id: ##All variables in a particular site
            site = self.sites_db[site_id]
            if site.is_running:
                print("Site ID: " + str(site.site_num))
                vars = site.variables
                site_vars = []
                for var in vars:
                    site_vars.append((var,vars[var].value))
                print(site_vars)

        elif not site_id: ##Particular Variable at all sites
            sites = self.var_sites[variable_id]
            sites_var = []
            for site in sites:
                print("Site ID: " + str(site.site_num))
                if site.is_running:
                    var = site.variable(variable_id)
                    sites_var.append((var.id,var.value))
                print(sites_var)