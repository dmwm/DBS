#!/usr/bin/env python
"""
This module provides MigrationRequests.UpdateRequestStatus data access object.
"""
__revision__ = "$Id: UpdateRequestStatus.py,v 1.2 2010/08/18 18:48:59 yuyi Exp $"
__version__ = "$Revision: 1.2 $"

from dbs.dao.Oracle.MigrationRequests.UpdateRequestStatus import UpdateRequestStatus as OraMigUpdateRequestStatus

class UpdateRequestStatus(OraMigUpdateRequestStatus):
    pass
		   
