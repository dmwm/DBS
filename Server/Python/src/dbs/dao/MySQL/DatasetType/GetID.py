#!/usr/bin/env python
"""
This module provides DatasetTYpe.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.2 2010/02/11 19:39:31 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

from dbs.dao.Oracle.DatasetType.GetID import GetID as OraDatasetTypeGetID

class GetID(OraDatasetTypeGetID):
            pass

