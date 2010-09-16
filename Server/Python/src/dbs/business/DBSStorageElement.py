#!/usr/bin/env python
"""
This module provides business object class to interact with StorageElement table. 
"""

__revision__ = "$Id: DBSStorageElement.py,v 1.2 2010/03/09 16:38:03 afaq Exp $"
__version__ = "$Revision: 1.2 $"

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
			ret=self.blkselist.execute(conn, block_name, tran)
		else:
        		ret=self.selist.execute(conn, se_name, tran)
		return ret

	except Exception, ex:
		raise ex
		
	finally:
		conn.close()

    

