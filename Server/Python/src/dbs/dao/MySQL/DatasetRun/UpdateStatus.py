#!/usr/bin/env python
"""
This module provides Dataset.UpdateStatus data access object.
"""
__revision__ = "$Id: UpdateStatus.py,v 1.2 2010/03/18 14:39:59 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from dbs.dao.Oracle.DatasetRun.UpdateStatus import UpdateStatus as OraDatasetRunUpdateStatus

class UpdateStatus(OraDatasetRunUpdateStatus):
    pass
