""" business unittests package"""

__revision__ = "$Id: __init__.py,v 1.1 2010/01/01 19:54:37 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

__all__ = ['DBSPrimaryDataset_t','DBSDataset_t', 'DBSBlock_t', 'DBSFile_t', 
           'DBSDatasetParent_t', 'DBSFileLumi_t', 'DBSFileParent_t', 
           'DBSOutputConfig_t']

import unittest

def run(logfile="", verbosity=2):
    print "\nBusiness Unittests:"
    loader = unittest.TestLoader().loadTestsFromModule
    modules = (__import__(m) for m in __all__)
    suites = [loader(m) for m in modules]
    alltests = unittest.TestSuite(suites)
    if logfile:
        fsock = open(logfile, 'w')
        runner = unittest.TextTestRunner(stream=fsock, verbosity=verbosity)
    else:
        runner = unittest.TextTestRunner(verbosity=verbosity)
    runner.run(alltests)

if __name__ == "__main__":
    run()