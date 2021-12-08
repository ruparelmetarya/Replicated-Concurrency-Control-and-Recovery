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


class DataManager:
    def __init__(self):
        self.sites_db = collections.defaultdict(Site)
        self.var_sites = collections.defaultdict(list)
        self._init_sites()
        self._init_var_sites()

    def _init_sites(self):
        for i in range(1, 11):
            self.sites_db[i] = Site(i)

    def _init_var_sites(self):
        for i in range(1, 21):
            var = "x" + str(i)
            if i % 2 == 0:
                self.var_sites[var] = [self.sites_db[j] for j in range(1, 11)]
            else:
                self.var_sites[var] = [self.sites_db[1 + (i % 10)]]

    def generate_cache_for_ro(self, trans: Transaction) -> None:
        for i, site in self.sites_db.items():
            if site.is_running:
                vars = site.variables
                for var in vars:
                    if site.is_valid_variable(var):
                        data = vars[var].value
                        trans.set_cache(var, data)

    def read(self, trans: Transaction, variable_id) -> (bool, list):
        if trans.read_only:
            if trans.cache and variable_id in trans.cache:
                print("Read: ", trans.cache[variable_id])
                return True, []
            elif trans.cache and variable_id not in trans.cache:
                return False, [-2]
            else:
                self.generate_cache_for_ro(trans)
                if variable_id in trans.cache:
                    print("Read: ", trans.cache[variable_id])
                    return True, []
                else:
                    trans.cache.clear()
                    return False, [-1]
        else:
            sites = self.var_sites.get(variable_id)
            print("stat: ", [site.is_running for site in sites])
            for site in sites:
                if site.is_running and site.is_valid_variable(variable_id):
                    locked_by = site.lock_table[variable_id].locks
                    if site.get_lock_type(variable_id) == LockType.WRITE:
                        if variable_id in locked_by:
                            print("Read: ", site.variables.get(variable_id).value)
                            return True, [site.site_num]
                        else:
                            print("returned here")
                            return False, locked_by
                    else:
                        site.add_variable_lock(variable_id, trans.ID, LockType.READ)
                        print("Read: ", site.variables.get(variable_id).value)
                        return True, [site.site_num]

            return False, [-1]

    def write(self, transaction_id, variable_id):
        sites = self.var_sites[variable_id]
        blocked_by = set()
        sites_updated = []
        write_success = True
        for site in sites:
            if site.is_running:
                sites_updated.append(site.site_num)
                locked_by = site.lock_table[variable_id].locks
                print("locked_by: ", locked_by)
                if site.get_lock_type(variable_id) == LockType.WRITE:
                    if variable_id in locked_by:
                        write_success = True
                    else:
                        print("returned here")
                        return False, locked_by
                elif site.get_lock_type(variable_id) == LockType.READ:
                    if locked_by[0] == transaction_id and len(locked_by) == 1:
                        continue
                    else:
                        write_success = False
                        for tran in locked_by:
                            if tran != transaction_id:
                                blocked_by.add(tran)
        if write_success:
            for site in sites:
                site.add_variable_lock(variable_id, transaction_id, LockType.WRITE)
                return True, sites_updated
        elif len(sites_updated) == 0:
            return False, [-1]
        else:
            blocked_by = list(blocked_by)
            return False, blocked_by

    def write_val_to_database(self, ID, val, sites_touched):
        for site in sites_touched:
            self.sites_db.get(site).write_to_variable(ID, val)

    def commit(self, commit_list):
        if len(commit_list) != 0:
            for ID, (val, sites_touched) in commit_list.items():
                self.write_val_to_database(ID, val, sites_touched)

    def release_locks(self, transaction_id, lock_dict):
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
        return set(all_freed)

    def fail(self, site_id):
        site = self.sites_db[site_id]
        if site.is_running:
            site.fail_site()

    def recover(self, site_id):
        site = self.sites_db[site_id]
        if not site.is_running:
            site.recover_site()

    def dump(self, site_id=None, variable_id=None):
        if not site_id and not variable_id:
            for i, site in self.sites_db.items():
                if site.is_running:
                    print("Site: " + str(site.site_num))
                    vars = site.variables
                    for var in vars:
                        print("Var: " + str(var) + " Value: " + str(vars[var].value))
        elif not variable_id:
            site = self.sites_db[site_id]
            if site.is_running:
                print("Site: " + str(site.site_num))
                vars = site.variables
                for var in vars:
                    print("Var: " + str(var) + " Value: " + str(vars[var].value))

        elif not site_id:
            sites = self.var_sites[variable_id]
            for site in sites:
                if site.is_running:
                    var = site.variable(variable_id)
                    print("Site: " + str(site.site_num) + " Var: " + str(var.id) + " Value: " + str(var.value))
