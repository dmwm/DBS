#!/usr/bin/env python
"""
This module provides FileType.GetID data access object.
Light dao object to get the id for a given FileType
"""
__revision__ = "$Id: GetID.py,v 1.2 2010/02/11 19:39:33 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from dbs.dao.Oracle.FileType.GetID import GetID as OraFileTypeGetID

class GetID(OraFileTypeGetID):
            pass

