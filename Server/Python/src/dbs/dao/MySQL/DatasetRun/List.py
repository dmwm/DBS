#!/usr/bin/env python
"""
This module provides DatasetRun.List data access object.
"""
__revision__ = "$Id: List.py,v 1.1 2010/03/01 20:55:52 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from dbs.dao.Oracle.DatasetRun.List import List as OraDatasetRunList

class List(OraDatasetRunList):
        pass

