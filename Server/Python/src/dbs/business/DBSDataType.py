#!/usr/bin/env python
"""
This module provides business object class to interact with primary_ds_types
table. 
"""
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class DBSDataType:
    """
    Primary dataset type business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger,
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.dataType = daofactory(classname="PrimaryDSType.List")

    def listDataType(self, dataType="", dataset=""):
        """
        List data-type/primary-ds-type 
        """
        try:
            conn = self.dbi.connection()
            if dataset and dataType:
                dbsExceptionHandler('dbsException-invalid-input',
                    "DBSDataType/listDataType. Data Type can be only searched by data_type or by dataset, not both.")
            else:
                result = self.dataType.execute(conn, dataType, dataset)
                return result
        finally:
            conn.close()
