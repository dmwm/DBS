#!/usr/bin/env python
"""
This module provides business object class to interact with RELEASE_VERSIONS table. 
"""
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionDef import DBSEXCEPTIONS

class DBSReleaseVersion:
    """
    Releaseversion business object class
    """
    def __init__(self, logger, dbi, owner):
	daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
	self.logger = logger
	self.dbi = dbi
	self.owner = owner

	self.releaseVersion = daofactory(classname="ReleaseVersion.List")

    def listReleaseVersions(self, release_version=""):
	"""
	List release versions
	"""
	try:
	    conn = self.dbi.connection()
	    plist = self.releaseVersion.execute(conn, release_version.upper())
            result=[{}]
            if plist:
                t=[]
                for i in plist:
                    for k, v in i.iteritems():
                        t.append(v)
                result[0]['release_version']=t
	    return result
	except Exception, ex:
	    raise ex
	finally:
	    conn.close()

