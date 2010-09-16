"""
web unittests package.
"""

__all__ = ['DBSMigrationValidation_t']

import unittest

def run(logfile="", verbosity=2):
    print "\nWeb Unittests:"
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
