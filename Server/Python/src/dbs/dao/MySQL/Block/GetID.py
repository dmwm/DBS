#!/usr/bin/env python
"""
This module provides Block.GetID data access object.
Light dao object to get the id for a give /primds/procds/tier#block
"""
__revision__ = "$Id: GetID.py,v 1.2 2010/02/11 19:39:29 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from dbs.dao.Oracle.Block.GetID import GetID as OraBlockGetID

class GetID(OraBlockGetID):
            pass

