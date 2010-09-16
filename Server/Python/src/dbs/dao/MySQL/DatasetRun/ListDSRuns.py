#!/usr/bin/env python
"""
This module provides DatasetRun.ListDSRuns data access object.
"""
__revision__ = "$Id: ListDSRuns.py,v 1.1 2010/03/01 21:59:12 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from dbs.dao.Oracle.DatasetRun.ListDSRuns import ListDSRuns as OraDatasetRunListDSRuns

class ListDSRuns(OraDatasetRunListDSRuns):
        pass

