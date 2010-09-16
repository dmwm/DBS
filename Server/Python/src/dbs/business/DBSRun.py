#!/usr/bin/env python
"""
This module provides business object class to interact with Dataset Run table. 
"""

__revision__ = "$Id: DBSRun.py,v 1.5 2010/03/15 16:30:04 afaq Exp $"
__version__ = "$Revision: 1.5 $"

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
	self.updatestatus = daofactory(classname='DatasetRun.UpdateStatus')
	
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
			if minRun==-1 and maxRun==-1:
			    raise Exception ("DBS Error: you must provide parameters to lisRuns call")
        		ret=self.runlist.execute(conn, minRun, maxRun, tran)
		return ret

	except Exception, ex:
		raise ex
		
	finally:
		conn.close()

    def updateStatus(self, dataset="", run_number=-1, complete=1):
        """
        Used to toggle the status of a dataset run complete flag 1/0
        """
        conn = self.dbi.connection()
        trans = conn.begin()

        try :
	    self.updatestatus.execute(conn, dataset, run_number, complete, trans)
	    trans.commit()
        except Exception, ex:
	    trans.rollback()
	    raise ex
        finally:
	    trans.close()
	    conn.close()
    
