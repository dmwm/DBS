#!/usr/bin/env python
"""
This module provides business object class to interact with OutputConfig. 
"""

__revision__ = "$Id: DBSOutputConfig.py,v 1.2 2009/12/21 21:06:57 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.DAOFactory import DAOFactory

class DBSOutputConfig:
    """
    Output Config business object class
    """
    def __init__(self, logger, dbi, owner):

        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.sm = daofactory(classname="SequenceManager")

	self.appid = daofactory(classname='ApplicationExecutable.GetID')
	self.verid = daofactory(classname='ReleaseVersion.GetID')
	self.hashid = daofactory(classname='ParameterSetHashe.GetID')

        self.appin = daofactory(classname='ApplicationExecutable.Insert')
        self.verin = daofactory(classname='ReleaseVersion.Insert')
        self.hashin = daofactory(classname='ParameterSetHashe.Insert')

        self.outmodin = daofactory(classname='OutputModuleConfig.Insert')

	
    def insertOutputConfig(self, businput):
	"""
	Method to insert the Output Config
	It first checks if release, app, and hash exists, if not insert them,
	and then insert the output module
		
	"""

	conn = self.dbi.connection()
	tran = conn.begin()

	try:
		try:
			businput["app_exec_id"] = self.appid.execute(businput["app_name"], conn, True)	
		except Exception, e:
			if str(e).find('does not exist') != -1:
				businput["app_exec_id"]=self.sm.increment("SEQ_AE", conn, True)
				appdaoinput={ "app_name" : businput["app_name"], "app_exec_id" : businput["app_exec_id"] }
				self.appin.execute(appdaoinput, conn, True)
			else : raise
		try:
			businput["release_version_id"] = self.verid.execute(businput["version"], conn, True)
		except Exception, e:
			if str(e).find('does not exist') != -1:
				businput["release_version_id"]=self.sm.increment("SEQ_RV", conn, True)
				verdaoinput={ "release_version_id" : businput["release_version_id"], "version" : businput["version"]    }
				self.verin.execute(verdaoinput, conn, True)
			else : raise
		try:
			businput["parameter_set_hash_id"] = self.hashid.execute(businput["hash"], conn, True)
		except Exception, e:
			if str(e).find('does not exist') != -1:
				businput["parameter_set_hash_id"]=self.sm.increment("SEQ_PSH", conn, True)
				pshdaoinput={"parameter_set_hash_id" : businput["parameter_set_hash_id"], "hash" : businput["hash"], "name" : "no_name" }
				self.hashin.execute(pshdaoinput, conn, True)
			else : raise
		# Proceed with o/p module insertion
		omcdaoinput ={
				"app_exec_id" : businput["app_exec_id"], 
				"release_version_id" : businput["release_version_id"],
				"parameter_set_hash_id" : businput["parameter_set_hash_id"],
				"output_module_label" : businput["output_module_label"], 
				"creation_date" : businput["creation_date"] , 
				"create_by" : businput["create_by"]
				}
		omcdaoinput["output_mod_config_id"]=self.sm.increment("SEQ_OMC", conn, True)
		self.outmodin.execute(omcdaoinput, conn, True)
		tran.commit()

	except Exception, e:
		tran.rollback()
		self.logger.exception(e)
		raise
	finally:
		conn.close()
