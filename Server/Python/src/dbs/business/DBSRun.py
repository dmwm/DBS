#!/usr/bin/env python
"""
This module provides business object class to interact with Dataset Run table. 
"""

__revision__ = "$Id: DBSRun.py,v 1.1 2010/03/01 20:55:52 afaq Exp $"
__version__ = "$Revision: 1.1 $"

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
			ret=self.dsrunlist.execute(dataset, minRun, maxRun, conn, tran)
		elif block_name:
			ret=self.blkrunlist.execute(block_name, minRun, maxRun, conn, tran)
		elif logical_file_name:
			ret=self.flrunlist.execute(logical_file_name, minRun, maxRun, conn, tran)
		else:
        		ret=self.runlist.execute(minRun, maxRun, conn, tran)
		return ret

	except Exception, ex:
		raise ex
		
	finally:
		conn.close()

    

