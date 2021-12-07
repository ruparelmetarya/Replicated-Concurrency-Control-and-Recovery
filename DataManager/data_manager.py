"""
Author: Safwan Mahmood
email: sm9453@nyu.edu
Created On: 11/27/2021
"""
from DataManager.site import Site
import collections
from TransactionManager import transaction
from Enums import lock_type
from DataManager.lock import Lock

class DataManager:
    def __init__(self):
        self.sites_db = collections.defaultdict(Site)
        self.var_sites = collections.defaultdict(list)
        self._init_sites()
        self._init_var_sites()

    def _init_sites(self):
        for i in range(1,11):
            self.sites_db[i] = Site(i)


    def _init_var_sites(self):
        for i in range(1,21):
            var = "x" + str(i)
            if i % 2 == 0:
                self.var_sites[var] = [self.sites_db[j] for j in range(1, 11)]
            else:
                self.var_sites[var] = [self.sites_db[1 + (i%10)]]


    def generateCacheForRO(self,trans):
        for i,site in self.sites_db.items():
            if site.is_running:
                vars = site.variables
                for var in vars:
                    if site.is_valid_variable(var):
                        data = vars[var].value
                        trans._cache[var] = data


    def read(self, trans, ID):
        if trans.read_only:
            if trans._cache and ID in trans._cache:
                return (True,[])
            elif trans._cache and not ID in trans._cache:
                return (False,[-2])
            else:
                self.generateCacheForRO(trans)
                if ID in trans._cache:
                    return (True, [])
                else:
                    trans._cache.clear()
                    return (False,[-1])
        else:
            sites = self.var_sites[ID]
            for site in sites:
                if site.is_running and site.is_valid_variable(ID):
                    locked_by = site.lock_table[ID].locks
                    if site.get_lock_type(ID) == lock_type.LockType.WRITE:
                        if ID in locked_by:
                            return (True, [site.site_num])
                        else:
                            return (False,locked_by)
                    else:
                        new_lock = Lock()
                        new_lock.add_lock(trans._ID,lock_type.LockType.READ)
                        site.set_lock_table(ID,new_lock)
                        return (True,site.site_num)

            return (False,[-1])



    def write(self,transID, ID):
        sites = self.var_sites[ID]
        blocked_by = set()
        sites_updated = []
        write_success = True
        for site in sites:
            if site.is_running:
                sites_updated.append(site.site_num)
                locked_by = site.lock_table[ID].locks
                if site.get_lock_type(ID) == lock_type.LockType.WRITE:
                    if ID in locked_by:
                        write_success = True
                    else:
                        return (False, locked_by)
                elif site.get_lock_type(ID) == lock_type.LockType.READ:
                    if locked_by[0] == transID and len(locked_by) == 1:
                        continue
                    else:
                        write_success = False
                        for tran in blocked_by:
                            if tran != transID:
                                blocked_by.add(tran)
        if write_success:
            for site in sites:
                new_lock = Lock()
                new_lock.add_lock(transID, lock_type.LockType.WRITE)
                site.set_lock_table(ID, new_lock)
                return (True,sites_updated)
        elif len(sites_updated) == 0:
            return (False,[-1])
        else:
            blocked_by = list(blocked_by)
            return (False,blocked_by)

    def writeValToDatabase(self, ID, val):
        sites = self.var_sites[ID]
        for site in sites:
            if site.is_running:
                site.write_to_variable(ID,val)

    def commit(self,transId, commitList):
        if len(commitList) != 0:
            for ID,val in commitList.items():
                self.writeValToDatabase(ID,val)

    def releaseLocks(self,transID, lockDict):
        all_freed = []
        for varID in lockDict:
            sites = self.var_sites[varID]
            locked = False
            for site in sites:
                if site.is_running:
                    site.remove_variable_lock(varID,transID)
                    if not site.is_variable_free(varID):
                        locked = True
            if not locked:
                all_freed.append(varID)
        return set(all_freed)

    def fail(self, siteNum):
        site = self.sites_db[siteNum]
        if site.is_running:
            site.fail_site()

    def recover(self, siteNum):
        site = self.sites_db[siteNum]
        if not site.is_running:
            site.recover_site()

    def dump(self, siteNum=None, ID=None):
        if not siteNum and not ID:
            for i,site in self.sites_db.items():
                if site.is_running:
                    print("Site: "+ str(site.site_num))
                    vars = site.variables
                    for var in vars:
                        print("Var: "+str(var)+" Value: "+str(vars[var].value))
        elif not ID:
            site = self.sites_db[siteNum]
            if site.is_running:
                print("Site: " + str(site.site_num))
                vars = site.variables
                for var in vars:
                    print("Var: "+str(var)+" Value: "+str(vars[var].value))

        elif not siteNum:
            sites = self.var_sites[ID]
            for site in sites:
                if site.is_running:
                    var = site.variable(ID)
                    print("Site: "+ str(site.site_num) +" Var: "+str(var.id)+" Value: "+str(var.value))


