#!/usr/bin/env python
"""
This module provides business object class to interact with Primary Dataset. 
"""

__revision__ = "$Id: DBSSite.py,v 1.1 2010/01/12 22:18:23 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.DAOFactory import DAOFactory

class DBSSite:
    """
    Site business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.sitelist = daofactory(classname="Site.List")
        self.sm = daofactory(classname="SequenceManager")
        self.sitein = daofactory(classname="Site.Insert")


    def listSites(self):
        """
        Returns all sites.
        """
        return self.sitelist.execute()


    def insertSite(self, businput):
        """
        Input dictionary has to have the following keys:
        site_name
        it builds the correct dictionary for dao input and executes the dao
        """
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
	    siteobj={
		"site_name" : businput["site_name"]
	    }
            businput["site_id"] = self.sm.increment("SEQ_SI", conn, True)
            self.sitein.execute(businput, conn, True)
            tran.commit()
        except Exception, ex:
            if str(ex).lower().find("unique constraint") != -1 :
                # already exists, lets fetch the ID
                self.logger.warning("Unique constraint violation being ignored...")
                self.logger.warning("%s" % ex)
                pass
            else:
                tran.rollback()
                self.logger.exception(ex)
                raise
        finally:
            conn.close()
