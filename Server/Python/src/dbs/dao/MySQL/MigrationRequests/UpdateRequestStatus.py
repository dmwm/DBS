#!/usr/bin/env python
"""
This module provides MigrationRequests.UpdateRequestStatus data access object.
"""
__revision__ = "$Id: UpdateRequestStatus.py,v 1.1 2010/08/18 18:46:38 yuyi Exp $"
__version__ = "$Revision: 1.1 $"

from dbs.dao.Oracle.MigrationRequests.UpdateRequestStatus import List as OraMigUpdateRequestStatus

class UpdateRequestStatus(OraMigUpdateRequestStatus):
    pass
		   
