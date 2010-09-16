#!/usr/bin/env python
""" DAO Object for PrimaryDatasets table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2010/02/11 19:39:34 afaq Exp $ "

from dbs.dao.Oracle.PrimaryDataset.Insert import Insert as OraPrimaryDatasetInsert

class Insert(OraPrimaryDatasetInsert):
    pass

