import sys, os
import fnmatch
from glob import glob
import unittest
from distutils.core import setup, Command

def get_relative_path():
    return os.path.dirname(os.path.abspath(os.path.join(os.getcwd(), sys.argv[0])))

def get_test_names(search_path, search_pattern, base_dir):
    excluded_files = []
    module_names = []

    for root, dirs, files in os.walk(search_path):
        for test_file in files:
            if fnmatch.fnmatch(test_file, search_pattern) and files not in excluded_files:
                filename = os.path.join(root, test_file)
                #Figure out the module name
                module_name = os.path.relpath(filename, base_dir).split('/')
                del module_name[-1] #remove filename from list
                module_name.append(os.path.splitext(test_file)[0])#add class name
                module_names.append('.'.join(module_name))
                
    return module_names
    
class TestCommand(Command):
    
    user_options =[('web=', None, 'Run unittests on web-layer options are DBSReader,DBSWriter, DBSMigrate'),
                   ('weball', None, 'Run unittests on web-layer'),
                   ('dao', None, 'Run unittests on dao-layer'),
                   ('business', None, 'Run unittests on business-layer'),
                   ('migration', None, 'Run unittests on migration service'),
                   ('allTests', None, 'Run unittests on all sub-layers'),
                   ('config=', None, 'Config file used for unittests'),
                   ('secrets=', None, 'Path to DBSSecrets file used for unittests')
                   ]

    description = """Test DBS3 using provided unittest, possible options are\n
                   --web=DBSReader, --web=DBSWriter or --web=DBSMigrate to run unittests on the web-layer\n
                   --weball to run reader and writer unittests on the web-layer\n
                   --dao to run unittests on the dao-layer\n
                   --business to run unittests on the business-layer\n
                   --migration to run unittests on migration service\n
                   --allTest to run unittests on all sub-layers\n
                   --config=<cfgfile> config file used for the unittests\n
                   --secrets=<path_to_secretfile> Path to DBSSecrets file used for unittests"""

    def initialize_options(self):
        self.web=None
        self.weball = False
        self.dao = False
        self.business = False
        self.migration = False
        self.allTests = False
        self.config = None
        self.secrets = "/data/auth/dbs"

    def finalize_options(self):
        #Check if environment is set-up correctly
        if not os.environ.get("DBS3_SERVER_ROOT"):
            if not os.environ.get("DBS3_ROOT"):  
                print """You have to source init.sh before running unittests\n
                If you are using rpm based development environment on a VM,\n
                try to source /data/current/apps/dbs/etc/profile.d/init.sh.\n
                If your are using development environment, source Server/Python/control/setup.sh.\n
                It will point to the base directory of your DBS3 installation to $DBS3_ROOT and that PYTHON_PATH\n
                is set-up correctly."""
                sys.exit(1)
            else:
                os.environ['DBS3_SERVER_ROOT'] = os.environ['DBS3_ROOT']

        if not self.config:
            print "Please, specify a config file using --config=<cfgfile> argument"
            sys.exit(3)

        if not self.secrets:
            print "Please, specify the path to the DBSSecrets file using --secrets=<path_to_secretsfile> argument."
            sys.exit(4)

        if not os.access(self.config, os.F_OK and os.R_OK):
            print "Cannot read config file %s.\n Please, ensure that it exists and you have the privileges to read it." %(self.config)
            sys.exit(5)

        if not self.web and not self.weball and not self.dao and not self.business and not self.migration and not self.allTests:
            print "Please, specify one of the following options.\n%s" % self.description
            sys.exit(6)

        if self.web not in [None, 'DBSWriter', 'DBSReader', 'DBSMigrate']:
            print "Valid options for --web are DBSReader, DBSWriter or DBSMigrate"
            sys.exit(7)

    def run(self):
          base_dir = get_relative_path()
          test_dir = os.path.join(base_dir, 'dbsserver_t', 'unittests')
          web_tests = os.path.join(test_dir, 'web_t')
          dao_tests = os.path.join(test_dir, 'dao_t')
          business_tests = os.path.join(test_dir, 'business_t')
          migration_tests = os.path.join(test_dir, 'components_t', 'migration_t')

          #prepare environment
          sys.path.append(self.secrets)
          os.environ['DBS_TEST_CONFIG'] = self.config

          #some tests need to be executed in a specific order, since they depend on each other
          #tests that are not mentioned have highest priority 0
          #tests are sorted ascending by priority
          test_priority_dao = {
                               'dbsserver_t.unittests.dao_t.Oracle_t.FileLumi_t.List_t' : 139,
                               'dbsserver_t.unittests.dao_t.Oracle_t.FileLumi_t.Insert_t' : 138,
                               'dbsserver_t.unittests.dao_t.Oracle_t.FileParent_t.List_t' : 129,
                               'dbsserver_t.unittests.dao_t.Oracle_t.FileParent_t.Insert_t' : 128,
                               'dbsserver_t.unittests.dao_t.Oracle_t.BlockParent_t.List_t' : 119,
                               'dbsserver_t.unittests.dao_t.Oracle_t.BlockParent_t.Insert_t' : 118,
                               'dbsserver_t.unittests.dao_t.Oracle_t.DatasetParent_t.List_t' : 109,
                               'dbsserver_t.unittests.dao_t.Oracle_t.DatasetParent_t.Insert_t' : 108,
                               'dbsserver_t.unittests.dao_t.Oracle_t.File_t.List_t' : 99,
                               'dbsserver_t.unittests.dao_t.Oracle_t.File_t.Insert_t' : 98,
                               'dbsserver_t.unittests.dao_t.Oracle_t.Block_t.List_t' : 89,
                               'dbsserver_t.unittests.dao_t.Oracle_t.Block_t.Insert_t' : 88,
                               'dbsserver_t.unittests.dao_t.Oracle_t.Dataset_t.Insert_t' : 78,
                               'dbsserver_t.unittests.dao_t.Oracle_t.Dataset_t.List_t' : 79
                               }

          if not self.allTests and not self.weball:
              if self.web=="DBSReader":
                  TestSuite = unittest.TestSuite()
                  module_names = get_test_names(web_tests, '[!.#]*ReaderModel_t.py', base_dir)
                  loadedTests = unittest.TestLoader().loadTestsFromNames(module_names)
                  TestSuite.addTests(loadedTests)
                  unittest.TextTestRunner(verbosity=2).run(TestSuite)

              if self.web=="DBSWriter":
                  TestSuite = unittest.TestSuite()
                  module_names = get_test_names(web_tests, '[!.#]*WriterModel_t.py', base_dir)
                  loadedTests = unittest.TestLoader().loadTestsFromNames(module_names)
                  TestSuite.addTests(loadedTests)
                  unittest.TextTestRunner(verbosity=2).run(TestSuite)

              if self.web=="DBSMigrate":
                  TestSuite = unittest.TestSuite()
                  module_names = get_test_names(web_tests, '[!.#]*MigrateModel_t.py', base_dir)
                  loadedTests = unittest.TestLoader().loadTestsFromNames(module_names)
                  TestSuite.addTests(loadedTests)
                  unittest.TextTestRunner(verbosity=2).run(TestSuite)

          if self.allTests or self.weball:
              TestSuite = unittest.TestSuite()
              module_names = get_test_names(web_tests, '[!.#]*_t.py', base_dir)
              #Sorted to run writer tests before reader test
              loadedTests = unittest.TestLoader().loadTestsFromNames(sorted(module_names, reverse=True))
              TestSuite.addTests(loadedTests)
              unittest.TextTestRunner(verbosity=2).run(TestSuite)

              #this will reset unique_id's used for the unittests
              del TestSuite

          if self.allTests or self.dao:
              TestSuite = unittest.TestSuite()
              module_names = get_test_names(dao_tests, '[!.#]*_t.py', base_dir)
              #some tests need to be executed in a specific order, since they depend on each other
              module_names = [(test_priority_dao.get(name, 0), name) for name in module_names]
              module_names.sort()
              module_names = [name for prio, name in module_names]

              loadedTests = unittest.TestLoader().loadTestsFromNames(module_names)
              TestSuite.addTests(loadedTests)
              unittest.TextTestRunner(verbosity=2).run(TestSuite)

              #this will reset unique_id's used for the unittests
              del TestSuite

          if self.allTests or self.business:
              TestSuite = unittest.TestSuite()
              module_names = get_test_names(business_tests, '[!.#]*_t.py', base_dir)
              #loadedTests = unittest.TestLoader().loadTestsFromNames(module_names)
              #TestSuite.addTests(loadedTests)
              #unittest.TextTestRunner(verbosity=2).run(TestSuite)

              #this will reset unique_id's used for the unittests
              del TestSuite

          if self.allTests or self.migration:
              TestSuite = unittest.TestSuite()
              module_names = get_test_names(migration_tests, '[!.#]*_t.py', base_dir)
              loadedTests = unittest.TestLoader().loadTestsFromNames(module_names)
              TestSuite.addTests(loadedTests)
              unittest.TextTestRunner(verbosity=2).run(TestSuite)

              #this will reset unique_id's used for the unittests
              del TestSuite

setup(name = 'dbs',
      version = '3.0',
      maintainer_email = 'hn-cms-dmDevelopment@cern.ch',
      cmdclass = { 'test_system': TestCommand})
