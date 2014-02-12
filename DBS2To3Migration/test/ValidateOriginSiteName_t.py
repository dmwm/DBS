"""
Unittests to validate translation of the origin_site_name between DBS 2 nad DBS 3
"""

import logging
import unittest

from DBSSqlQueries import DBSSqlQueries

try:
    from DBSSecret import DBS2Secret
    from DBSSecret import DBS3Secret
except:
    msg = """You need to put a DBSSecret.py in your directory. It has to have the following structure:\n
              DBS2Secret = {'connectUrl' : {
                            'reader' : 'oracle://reader:passwd@instance'
                            },
                            'databaseOwner' : 'owner'}
              DBS3Secret = {'connectUrl' : {
                            'reader' : 'oracle://reader:passwd@instance'
                            },
                            'databaseOwner' : 'owner'}"""
    print msg
    raise


class CompareOriginSiteName(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        super(CompareOriginSiteName, self).__init__(methodName)
        ownerDBS3 = DBS3Secret['databaseOwner']
        connectUrlDBS3 = DBS3Secret['connectUrl']['reader']

        ownerDBS2 = DBS2Secret['databaseOwner']

        logger = logging.getLogger()

        self.dbssqlqueries = DBSSqlQueries(logger, connectUrlDBS3, ownerDBS3, ownerDBS2)

    def test_origin_site_name(self):
        resultsUnion = self.dbssqlqueries.originSiteName(sort=False)

        self.assertEqual(len(resultsUnion), 0)


if __name__ == '__main__':
    TestSuite = unittest.TestSuite()
    TestSuite.addTest(CompareOriginSiteName('test_origin_site_name'))

    unittest.TextTestRunner(verbosity=2).run(TestSuite)