#!/usr/bin/env python
"""
This module provides business object class to interact with Dataset Run table. 
"""

__revision__ = "$Id: DBSRun.py,v 1.2 2010/03/08 20:22:23 yuyi Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.DAOFactory import DAOFactory

class DBSRun:
    """
    Site business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.dsrunlist = daofactory(classname="DatasetRun.ListDSRuns")
        self.blkrunlist = daofactory(classname="DatasetRun.ListBlockRuns")
        self.flrunlist = daofactory(classname="DatasetRun.ListFileRuns")
        self.runlist = daofactory(classname="DatasetRun.List")

    def listRuns(self, dataset="", block_name="", logical_file_name="", minRun=-1, maxRun=-1):
        """
        List run known to DBS.
        """
	try:
		conn = self.dbi.connection()
		tran=False

		if dataset:
			ret=self.dsrunlist.execute(conn, dataset, minRun, maxRun, tran)
		elif block_name:
			ret=self.blkrunlist.execute(conn, block_name, minRun, maxRun, tran)
		elif logical_file_name:
			ret=self.flrunlist.execute(conn, logical_file_name, minRun, maxRun, tran)
		else:
        		ret=self.runlist.execute(minRun, maxRun, conn, tran)
		return ret

	except Exception, ex:
		raise ex
		
	finally:
		conn.close()

    

