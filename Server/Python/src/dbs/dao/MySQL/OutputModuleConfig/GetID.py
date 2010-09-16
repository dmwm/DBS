#!/usr/bin/env python
"""
This module provides ApplicationExecutable.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.2 2010/02/11 19:39:33 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from dbs.dao.Oracle.OutputModuleConfig.GetID import GetID as OraOutputModuleConfigGetID

class GetID(OraOutputModuleConfigGetID):
            pass

