#!/usr/bin/env python
#pylint: disable=C0103
"""
This module provides business object class to interact with the
RELEASE_VERSIONS table. 
"""
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class DBSReleaseVersion:
    """
    Releaseversion business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger,
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        self.owner = owner

        self.releaseVersion = daofactory(classname="ReleaseVersion.List")

    def listReleaseVersions(self, release_version="", dataset=''):
        """
        List release versions
        """
        if dataset and ('%' in dataset or '*' in dataset):
            dbsExceptionHandler('dbsException-invalid-input',
                " DBSReleaseVersion/listReleaseVersions. No wildcards are" +
                " allowed in dataset.\n.")
        try:
            conn = self.dbi.connection()
            plist = self.releaseVersion.execute(conn, release_version.upper(), dataset )
            result = [{}]
            if plist:
                t = []
                for i in plist:
                    for k, v in i.iteritems():
                        t.append(v)
                result[0]['release_version'] = t
            return result
        finally:
            conn.close()
