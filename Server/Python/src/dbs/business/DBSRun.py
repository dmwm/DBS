#!/usr/bin/env python
"""
This module provides business object class to interact with Dataset Run table. 
"""

__revision__ = "$Id: DBSRun.py,v 1.8 2010/03/18 18:53:42 afaq Exp $"
__version__ = "$Revision: 1.8 $"

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

    def listRuns(self, dataset="", block_name="", logical_file_name="", minrun=-1, maxRun=-1):
        """
        List run known to DBS.
        """
	try:
		conn = self.dbi.connection()
		tran=False

		if dataset:
			ret=self.dsrunlist.execute(conn, dataset, minrun, maxrun, tran)
		elif block_name:
			ret=self.blkrunlist.execute(conn, block_name, minrun, maxrun, tran)
		elif logical_file_name:
			ret=self.flrunlist.execute(conn, logical_file_name, minrun, maxrun, tran)
		else:
        		ret=self.runlist.execute(conn, minrun, maxRun, tran)
		return ret

	except Exception, ex:
		raise ex
		
	finally:
		conn.close()

   
