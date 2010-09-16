#!/usr/bin/env python
"""
This module provides File.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.2 2010/02/11 19:39:31 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from dbs.dao.Oracle.File.GetID import GetID as OraFileGetID

class GetID(OraFileGetID):
            pass

