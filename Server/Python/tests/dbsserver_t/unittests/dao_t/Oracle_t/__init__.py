"""
Oracle DAO unittests package.
"""

__revision__ = "$Id: __init__.py,v 1.1 2010/01/01 19:54:40 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

__all__ = ['PrimaryDataset_t','Dataset_t', 'Block_t', 'File_t', 
           'DatasetParent_t', 'OutputModuleConfig_t', 'FileLumi_t', 
           'FileParent_t']

import sys
import unittest

def modules():
    modules = []
    for dao_t in __all__:
        __import__(dao_t)
        dao_t_modules = sys.modules[dao_t].__all__
        for t in dao_t_modules:
            daotestname = "%s.%s" % (dao_t, t)
            __import__(daotestname)
            modules.append(sys.modules[daotestname])
    return modules

def run(logfile="", verbosity=2):
    print "\nDAO Unittests"
    
    loader = unittest.TestLoader().loadTestsFromModule
    suites = [loader(m) for m in modules()]
    alltests = unittest.TestSuite(suites)
    if logfile:
        fsock = open(logfile, 'w')
        runner = unittest.TextTestRunner(stream=fsock, verbosity=verbosity)
    else:
        runner = unittest.TextTestRunner(verbosity=verbosity)
    runner.run(alltests)    

if __name__ == "__main__":
    run()
    