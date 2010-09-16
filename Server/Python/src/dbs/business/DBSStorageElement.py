#!/usr/bin/env python
"""
This module provides business object class to interact with StorageElement table. 
"""

__revision__ = "$Id: DBSStorageElement.py,v 1.1 2010/03/02 21:12:58 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.DAOFactory import DAOFactory

class DBSStorageElement:
    """
    StorageElement business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.selist = daofactory(classname="StorageElement.List")
        self.blkselist = daofactory(classname="StorageElement.ListBlockSE")

    def listStorageElements(self, block_name="", se_name=""):
        """
        List Storage Elements known to DBS.
        """

	try:
		conn = self.dbi.connection()
		tran=False

		if block_name:
			ret=self.blkselist.execute(block_name, conn, tran)
		else:
        		ret=self.selist.execute(se_name, conn, tran)
		return ret

	except Exception, ex:
		raise ex
		
	finally:
		conn.close()

    

