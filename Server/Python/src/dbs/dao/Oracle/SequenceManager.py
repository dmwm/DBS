#!/usr/bin/env python
"""
This module manages sequences.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class  SequenceManager(DBFormatter):
    """
    Sequence Manager class.
    """
    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.logger = logger

    def increment(self, conn, seqName, transaction = False, incCount=1):
        """
        increments the sequence `seqName` by default `Incremented by`
        and returns its value
        incCount: is UNUSED variable in Oracle implementation
        """

        #FIXME: Do we need to lock the tables here?

        sql = "select %s%s.nextval as val from dual" % (self.owner, seqName)
        result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        resultlist = self.formatDict(result)
        return resultlist[0]['val']
