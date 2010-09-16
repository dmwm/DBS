#!/usr/bin/env python
""" DAO Object for DbsVersions table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2010/02/11 19:39:31 afaq Exp $ "

from dbs.dao.Oracle.DbsVersion.Insert import Insert as OraDbsVersionInsert

class Insert(OraDbsVersionInsert):
            pass

