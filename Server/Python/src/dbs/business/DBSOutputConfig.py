#!/usr/bin/env python
"""
This module provides business object class to interact with OutputConfig. 
"""

__revision__ = "$Id: DBSOutputConfig.py,v 1.15 2010/08/19 15:22:18 yuyi Exp $"
__version__ = "$Revision: 1.15 $"

from WMCore.DAOFactory import DAOFactory
from sqlalchemy import exceptions

class DBSOutputConfig:
    """
    Output Config business object class
    """
    def __init__(self, logger, dbi, owner):

        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner
        
        self.outputmoduleconfiglist = daofactory(classname='OutputModuleConfig.List')

        self.sm = daofactory(classname="SequenceManager")

        self.appid = daofactory(classname='ApplicationExecutable.GetID')
        self.verid = daofactory(classname='ReleaseVersion.GetID')
        self.hashid = daofactory(classname='ParameterSetHashe.GetID')

        self.appin = daofactory(classname='ApplicationExecutable.Insert')
        self.verin = daofactory(classname='ReleaseVersion.Insert')
        self.hashin = daofactory(classname='ParameterSetHashe.Insert')

        self.outmodin = daofactory(classname='OutputModuleConfig.Insert')
        
    def listOutputConfigs(self, dataset="", logical_file_name="", 
                         release_version="", pset_hash="", app_name="", output_module_label="", block_id=0):
	try:
	    conn=self.dbi.connection()
	    result = self.outputmoduleconfiglist.execute(conn, dataset,
                                                   logical_file_name,
                                                   app_name,
                                                   release_version,
                                                   pset_hash,
                                                   output_module_label, block_id)
	    conn.close()
	    return result
        except Exception, ex:
            raise ex
	finally:
	    if conn: 
		conn.close()
    
    def insertOutputConfig(self, businput):
        """
        Method to insert the Output Config
        It first checks if release, app, and pset_hash exists, if not insert them,
        and then insert the output module
		
        """

        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            businput["app_exec_id"] = self.appid.execute(conn, businput["app_name"], tran)	
	    if businput["app_exec_id"] == -1:
	        businput["app_exec_id"] = self.sm.increment(conn, "SEQ_AE",tran)
                appdaoinput = { "app_name" : businput["app_name"], 
                                    "app_exec_id" : businput["app_exec_id"] }
                self.appin.execute(conn, appdaoinput, tran)
            businput["release_version_id"] = self.verid.execute(conn, businput["release_version"], tran)
	    if businput["release_version_id"] == -1:
		businput["release_version_id"] = self.sm.increment(conn, "SEQ_RV", tran)
                verdaoinput = { 
		    "release_version" : businput["release_version"],
		    "release_version_id" : businput["release_version_id"]
		    }
                self.verin.execute(conn, verdaoinput, tran)
            businput["parameter_set_hash_id"] = self.hashid.execute(conn, businput["pset_hash"], tran)
	    if businput["parameter_set_hash_id"] == -1:
		businput["parameter_set_hash_id"] = self.sm.increment(conn, "SEQ_PSH", tran)
		pshdaoinput = {"parameter_set_hash_id" : businput["parameter_set_hash_id"], 
                               "pset_hash" : businput["pset_hash"], 
                               "name" : "no_name" }
                self.hashin.execute(conn, pshdaoinput, tran)
            # Proceed with o/p module insertion
            omcdaoinput = {
			"app_exec_id" : businput["app_exec_id"], 
			"release_version_id" : businput["release_version_id"],
			"parameter_set_hash_id" : businput["parameter_set_hash_id"],
			"output_module_label" : businput["output_module_label"], 
			"creation_date" : businput["creation_date"] , 
			"create_by" : businput["create_by"]
			}
            omcdaoinput["output_mod_config_id"] = self.sm.increment(conn, "SEQ_OMC", tran)
            self.outmodin.execute(conn, omcdaoinput, tran)
            tran.commit()

	except exceptions.IntegrityError, ex:
	    if str(ex).find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
		pass
	    else: 
	        raise
		
        except Exception, e:
		tran.rollback()
		self.logger.exception(e)
		raise
        finally:
            if conn:
		conn.close()
