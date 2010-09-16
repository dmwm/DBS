#!/usr/bin/env python
"""
This module provides FileParentBlock.List data access object.

Given the ID of a File, returns a LIST of the dicts containing IDs 
[{block_id, dataset_id},....] of the Parent BLOCK of the 
Block containing THIS file.
"""
__revision__ = "$Id: List.py,v 1.2 2010/02/11 19:39:32 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from dbs.dao.Oracle.FileParentBlock.List import List as OraFileParentBlockList

class List(OraFileParentBlockList):
        pass

