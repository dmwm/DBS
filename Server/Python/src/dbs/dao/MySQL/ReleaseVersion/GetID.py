#!/usr/bin/env python
"""
This module provides ReleaseVersion.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.2 2010/02/11 19:39:35 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from dbs.dao.Oracle.ReleaseVersion.GetID import GetID as OraReleaseVersionGetID

class GetID(OraReleaseVersionGetID):
            pass

