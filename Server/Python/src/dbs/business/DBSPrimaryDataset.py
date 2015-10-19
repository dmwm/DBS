#!/usr/bin/env python
#pylint: disable=C0103
"""
This module provides business object class to interact with Primary Dataset.
"""

__revision__ = "$Id: DBSPrimaryDataset.py,v 1.21 2010/05/19 16:21:05 yuyi Exp $"
__version__ = "$Revision: 1.21 $"

from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsUtils import dbsUtils

class DBSPrimaryDataset:
    """
    Primary Dataset business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger,
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner
        self.primdslist = daofactory(classname="PrimaryDataset.List")
        self.sm = daofactory(classname="SequenceManager")
        self.primdstypeList = daofactory(classname="PrimaryDSType.List")
        self.primdsin = daofactory(classname="PrimaryDataset.Insert")

    def listPrimaryDatasets(self, primary_ds_name="", primary_ds_type=""):
        """
        Returns all primary dataset if primary_ds_name or primary_ds_type are not passed.
        """
        conn = self.dbi.connection()
        try:
            result = self.primdslist.execute(conn, primary_ds_name, primary_ds_type)
            if conn: conn.close()
            return result
        finally:
            if conn:
                conn.close()

    def listPrimaryDSTypes(self, primary_ds_type="", dataset=""):
        """
        Returns all primary dataset types if dataset or primary_ds_type are not passed.
        """
        conn = self.dbi.connection()
        try:
            result = self.primdstypeList.execute(conn, primary_ds_type, dataset)
            if conn: conn.close()
            return result
        finally:
            if conn:
                conn.close()

    def insertPrimaryDataset(self, businput):
        """
        Input dictionary has to have the following keys:
        primary_ds_name, primary_ds_type, creation_date, create_by.
        it builds the correct dictionary for dao input and executes the dao
        """
        conn = self.dbi.connection()
        tran = conn.begin()
        #checking for required fields
        if "primary_ds_name" not in businput:
            dbsExceptionHandler("dbsException-invalid-input",
                " DBSPrimaryDataset/insertPrimaryDataset. " +
                "Primary dataset Name is required for insertPrimaryDataset.")
        try:
            businput["primary_ds_type_id"] = (self.primdstypeList.execute(conn, businput["primary_ds_type"]
                ))[0]["primary_ds_type_id"]
            del businput["primary_ds_type"]
            businput["primary_ds_id"] = self.sm.increment(conn, "SEQ_PDS")
            self.primdsin.execute(conn, businput, tran)
            tran.commit()
            tran = None
        except KeyError as ke:
            dbsExceptionHandler("dbsException-invalid-input",
                " DBSPrimaryDataset/insertPrimaryDataset. Missing: %s" % ke)
            self.logger.warning(" DBSPrimaryDataset/insertPrimaryDataset. Missing: %s" % ke)
        except IndexError as ie:
            dbsExceptionHandler("dbsException-missing-data",
                " DBSPrimaryDataset/insertPrimaryDataset. %s" % ie)
            self.logger.warning(" DBSPrimaryDataset/insertPrimaryDataset. Missing: %s" % ie)
        except Exception as ex:
            if (str(ex).lower().find("unique constraint") != -1 or
                str(ex).lower().find("duplicate") != -1):
                self.logger.warning("DBSPrimaryDataset/insertPrimaryDataset:" +
                        " Unique constraint violation being ignored...")
                self.logger.warning(ex)
            else:
                if tran:
                    tran.rollback()
                if conn: conn.close()
                raise
        finally:
            if tran:
                tran.rollback()
            if conn:
                conn.close()
