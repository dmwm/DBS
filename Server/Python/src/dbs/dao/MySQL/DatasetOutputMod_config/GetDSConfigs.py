#!/usr/bin/env python
"""
This module provides GetDSConfigs data access object.
Light dao object to get the id for a give /primds/procds/tier
"""
__revision__ = "$Id: GetDSConfigs.py,v 1.1 2010/03/08 23:12:35 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from dbs.dao.Oracle.DatasetOutputMod_config.GetDSConfigs import GetDSConfigs as OraGetDSConfigs

class GetDSConfigs(OraGetDSConfigs):
            pass

