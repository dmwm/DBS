#!/usr/bin/env python
"""
This module provides Dataset.UpdateStatus data access object.
"""
__revision__ = "$Id: UpdateStatus.py,v 1.1 2010/03/08 23:12:38 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from dbs.dao.Oracle.Dataset.UpdateStatus import UpdateStatus as OraDatasetUpdateStatus

class UpdateStatus(OraDatasetUpdateStatus):
    pass
