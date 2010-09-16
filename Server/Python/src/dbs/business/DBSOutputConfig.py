#!/usr/bin/env python
"""
This module provides business object class to interact with OutputConfig. 
"""

__revision__ = "$Id: DBSOutputConfig.py,v 1.1 2009/12/18 22:46:04 afaq Exp $"
__version__ = "$Revision: 1.1 $"

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
		businput["app_exec_id"] = self.appid.execute(businput["app_name"], conn, True)	
		if businput["app_exec_id"] in ('', None):
			businput["app_exec_id"]=self.sm.increment("SEQ_AE", conn, True)
			appdaoinput={businput["app_name"]}
			self.appin.execute(appdaoinput, conn, True)

		businput["release_version_id"] = self.verid.execute(businput["version"], conn, True)
		if businput["release_version_id"] in ('', None):
			verdaoinput={}
			businput["release_version_id"]=self.sm.increment("SEQ_RV", conn, True)
			self.verin.execute(verdaoinput, conn, True)

		businput["parameter_set_hash_id"] = self.hashid.execute(businput["hash"], conn, True)
		if businput["parameter_set_hash_id"] in ('', None):
			pshdaoinput={}
			businput["parameter_set_hash_id"]=self.sm.increment("SEQ_PSH", conn, True)
			#if name is provided pshdaoinput["name"]=
			self.hashin.execute(pshdaoinput, conn, True)

		omcdaoinput ={}
		omcdaoinput["output_mod_config_id"]=self.sm.increment("SEQ_OMC", conn, True)
		self.outmodin.execute(omcdaoinput, conn, True)
		tran.commit()

	except Exception, e:
		tran.rollback()
		self.logger.exception(e)
		raise
	finally:
		conn.close()

	

