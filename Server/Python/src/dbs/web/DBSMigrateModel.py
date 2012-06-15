#!/usr/bin/env python
#pylint: disable=C0103
"""
DBS Migration Service Client Interface Rest Model module
"""

import cjson
import traceback

from cherrypy import request, tools, HTTPError
#from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsUtils import dbsUtils
from dbs.utils.dbsException import dbsException, dbsExceptionCode
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSInputValidation import *
from WMCore.WebTools.RESTModel import RESTModel
from dbs.business.DBSMigrate import DBSMigrate

__server__version__ = "$Name:  $"

#Necessary for sphinx documentation and server side unit tests.
if not getattr(tools,"secmodv2",None):
    class FakeAuthForDoc(object):
        def __init__(self,*args,**kwargs):
            pass

        def callable(self, role=[], group=[], site=[], authzfunc=None):
            pass

    tools.secmodv2 = FakeAuthForDoc()

def authInsert(user, role, group, site):
    """
    Authorization function for general insert  
    """
    if not role: return True
    for k, v in user['roles'].iteritems():
        for g in v['group']:
            if k in role.get(g, '').split(':'):
                return True
    return False


class DBSMigrateModel(RESTModel):
    """
    DBS Migration Service Web Model
    """
    
    def __init__(self, config):
        """
        All parameters are provided through DBSConfig module
        """
        dbowner = config.database.dbowner

        RESTModel.__init__(self, config)
        self.methods = {'GET':{}, 'PUT':{}, 'POST':{}, 'DELETE':{}}
        self.security_params = config.security.params
       
        self._addMethod('POST', 'submit', self.submit,  secured=True,
            security_params={'role':self.security_params, 'authzfunc':authInsert})
        self._addMethod('POST', 'remove', self.remove, secured=True,
            security_params={'role':self.security_params, 'authzfunc':authInsert})
        self._addMethod('GET', 'status', self.status, args=['migration_rqst_id','block_name','dataset', 'user'])
        
        self.dbsMigrate = DBSMigrate(self.logger, self.dbi, dbowner)
    
    def submit(self):
        """
        Interface for submitting a migration request.
        Required input keys:
        MIGRATION_URL: The source DBS url for migration.
        MIGRATION_INPUT: The block or dataset names to be migrated.
        """
        body = request.body.read()
        indata = cjson.decode(body)
        indata = validateJSONInputNoCopy("migration_rqst",indata)
        indata.update({"creation_date": dbsUtils().getTime(),
                "last_modification_date" : dbsUtils().getTime(),
                "create_by" : dbsUtils().getCreateBy() ,
                "last_modified_by" : dbsUtils().getCreateBy(),
                "migration_status": 0})
        return self.dbsMigrate.insertMigrationRequest(indata)
    
    @inputChecks(migration_rqst_id=(long,int,str), block_name=str, dataset=str, user=str)
    def status(self, migration_rqst_id="", block_name="", dataset="", user=""):
        """
        Interface to query status of a migration request
        In this preference order of input parameters :
            migration_rqst_id, block, dataset, user
            (if multi parameters are provided, only the precedence order is followed)
        """
        return self.dbsMigrate.listMigrationRequests(migration_rqst_id,
            block_name, dataset, user)
    
    def remove(self):
        """
        Interface to remove a migration request from the queue.
        Only FAILED/3 and PENDING/0 requests can be removed
        (running and sucessed requests cannot be removed)

        """
        body = request.body.read()
        indata = cjson.decode(body)
        indata = validateJSONInputNoCopy("migration_rqst",indata)
        return self.dbsMigrate.removeMigrationRequest(indata['migration_rqst_id'])
        

    def getServerVersion(self):
        """
        Reading from __version__ tag, determines the version of the DBS Server
        """
        version = __server__version__.replace("$Name: ", "")
        version = version.replace("$", "")
        version = version.strip()
        return version

    def getHelp(self, call=""):
        if call:
            params = self.methods['GET'][call]['args']
            doc = self.methods['GET'][call]['call'].__doc__
            return dict(params=params, doc=doc)
        else:
            return self.methods['GET'].keys()


    def getServerInfo(self):
        """
        Method that provides information about DBS Migration Server to the clients
        The information includes
        * Server Version - CVS Tag
        * Schema Version - Version of Schema this DBS instance is working with
        * ETC - TBD
        """
        ret = {}
        ret["tagged_version"] = self.getServerVersion()
        ret["schema"] = self.dbsStatus.getSchemaStatus()
        ret["components"] = self.dbsStatus.getComponentStatus()
        return ret 
