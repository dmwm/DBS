#!/usr/bin/env python
"""
This module manages sequences.
"""

__revision__ = "$Id: SequenceManager.py,v 1.2 2009/11/03 16:41:25 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.Database.DBFormatter import DBFormatter

class  SequenceManager(DBFormatter):
    """
    Sequence Manager class.
    """
    def __init__(self, logger, dbi):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username

    def increment(self, seqName, conn = None, transaction = False):
        """
        increments the sequence `seqName` by default `Incremented by`
        and returns its value
        """
        #I am getting the increment_by from the database now. might as well just
        # store all sequence related info in a separate module
        sql = "select %s%s.nextval as val from dual" % (self.owner, seqName)
        result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        resultlist = self.formatDict(result)
        return resultlist[0]['val']

    def incrementN(self, seqName, N, conn = None, transaction = False):
        """
        increment sequence by n default incremented_by, so that
        n*incremented_by > N and returns the list of id's
        """
        sqlinc = "select increment_by from user_sequences where sequence_name = '%s'" % seqName
        result = self.dbi.processData(sqlinc, conn=conn, transaction = transaction)
        increment = self.formatDict(result)[0]['increment_by']
        nNext = int(N/increment)
        sqlnext = "select %s%s.nextval as val from dual" % (self.owner, seqName)
        for i in range(nNext+1):
            result = self.dbi.processData(sqlnext, conn=conn, transaction = transaction)
        lastval = self.formatDict(result)[0]["val"]
        return range(lastval-N, lastval)
        
    def currentid(self, seqName):
        """
        returns current value of sequence `seqName`
        not working for now.
        """
        sql = "select %s.currval as val from dual" % (seqName)
        result = self.dbi.processData(sql)
        resultlist = self.formatDict(result)
        return resultlist[0]['val']
