#!/usr/bin/env python
#pylint: disable=C0103
"""
This module provides business object class to interact with
DATASET_ACCESS_TYPES table.
"""
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler


class DBSDatasetAccessType:
    """
    DatasetAccessType business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger,
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.datasetAccessType = daofactory(classname="DatasetType.List")

    def listDatasetAccessTypes(self, dataset_access_type=""):
        """
        List dataset access types
        """
        if isinstance(dataset_access_type, basestring):
            try:
                dataset_access_type = str(dataset_access_type)
            except:    
                dbsExceptionHandler('dbsException-invalid-input', 'dataset_access_type given is not valid : %s' %dataset_access_type)
        else:
            dbsExceptionHandler('dbsException-invalid-input', 'dataset_access_type given is not valid : %s' %dataset_access_type)
        conn = self.dbi.connection()
        try:
            plist = self.datasetAccessType.execute(conn, dataset_access_type.upper())
            result = [{}]
            if plist:
                t = []
                for i in plist:
                    for k, v in i.iteritems():
                        t.append(v)
                result[0]['dataset_access_type'] = t
            return result
        finally:
            if conn:
                conn.close()

