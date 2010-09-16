#!/usr/bin/env python
"""
This module provides Dataset.GetID data access object.
Light dao object to get the id for a give /primds/procds/tier
"""
__revision__ = "$Id: GetID.py,v 1.2 2010/02/11 19:39:30 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from dbs.dao.Oracle.Dataset.GetID import GetID as OraDatasetGetID

class GetID(OraDatasetGetID):
            pass

