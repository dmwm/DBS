#!/usr/bin/env python
"""
This module provides DatasetTYpe.GetID data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter

from dbs.dao.Oracle.DatasetType.GetID import GetID as OraDatasetTypeGetID

class GetID(OraDatasetTypeGetID):
            pass

