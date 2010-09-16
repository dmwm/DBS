#!/usr/bin/env python
"""
This module provides business object class to interact with Primary Dataset. 
"""

__revision__ = "$Id: DBSPrimaryDataset.py,v 1.18 2010/03/09 16:38:03 afaq Exp $"
__version__ = "$Revision: 1.18 $"

from WMCore.DAOFactory import DAOFactory

class DBSPrimaryDataset:
    """
    Primary Dataset business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner
	#print "Primary DS Biz owner %s"  %owner

        self.primdslist = daofactory(classname="PrimaryDataset.List")
        self.sm = daofactory(classname="SequenceManager")
        self.primdstypeList = daofactory(classname="PrimaryDSType.List")
        self.primdsin = daofactory(classname="PrimaryDataset.Insert")


    def listPrimaryDatasets(self, primary_ds_name=""):
        """
        Returns all primary datasets if primary_ds_name is not passed.
        """
	conn=self.dbi.connection()
        result= self.primdslist.execute(conn, primary_ds_name)
	conn.close()
	return result

    def insertPrimaryDataset(self, businput):
        """
        Input dictionary has to have the following keys:
        primary_ds_name, primary_ds_type, creation_date, create_by
        it builds the correct dictionary for dao input and executes the dao
        """
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
	    #import threading
	    #a = threading.currentThread()
	    #self.logger.warning("\n####### %s #######\n" %str(a.dialect))
            businput["primary_ds_type_id"] = (self.primdstypeList.execute(conn, businput["primary_ds_type"], transaction=tran))[0]["primary_ds_type_id"] 
            del businput["primary_ds_type"]
            businput["primary_ds_id"] = self.sm.increment(conn, "SEQ_PDS", tran)
            self.primdsin.execute(conn, businput, tran)
            tran.commit()
        except IndexError:
            self.logger.exception( "DBS Error: Index error raised")
            raise 
        except Exception, ex:
            if str(ex).lower().find("unique constraint") != -1 \
			    or str(ex).lower().find("duplicate") != -1:
                self.logger.warning("Unique constraint violation being ignored...")
                self.logger.warning("%s" % ex)
                pass
            else:
                tran.rollback()
                self.logger.exception(ex)
                raise 
        finally:
            conn.close()
