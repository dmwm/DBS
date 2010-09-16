#!/usr/bin/env python
"""
This module provides business object class to interact with Primary Dataset. 
"""

__revision__ = "$Id: DBSPrimaryDataset.py,v 1.21 2010/05/19 16:21:05 yuyi Exp $"
__version__ = "$Revision: 1.21 $"

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


    def listPrimaryDatasets(self, primary_ds_name="", primary_ds_type=""):
        """
        Returns all primary datasets if primary_ds_name or primary_ds_type are not passed.
        """
	try:
	    conn=self.dbi.connection()
	    result= self.primdslist.execute(conn, primary_ds_name, primary_ds_type)
	    conn.close()
	    return result
        except Exception, ex:
            raise ex
	finally:
	    conn.close()

    def insertPrimaryDataset(self, businput):
        """
        Input dictionary has to have the following keys:
        primary_ds_name, primary_ds_type, creation_date, create_by
        it builds the correct dictionary for dao input and executes the dao
        """
        conn = self.dbi.connection()
        tran = conn.begin()
	#checking for required fields
	if "primary_ds_name" not in businput:
	    self.logger.exception( "DBSException: Primary dataset Name is required for insertPrimaryDataset")
	    raise Exception ( "DBSException: Primary dataset Name is required for insertPrimaryDataset.")
        try:
            businput["primary_ds_type_id"] = (self.primdstypeList.execute(conn, businput["primary_ds_type"], 
	                transaction=tran))[0]["primary_ds_type_id"] 
            del businput["primary_ds_type"]
            businput["primary_ds_id"] = self.sm.increment(conn, "SEQ_PDS", tran)
            self.primdsin.execute(conn, businput, tran)
            tran.commit()
        except IndexError:
            self.logger.exception( "DBSError: Index error raised")
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
