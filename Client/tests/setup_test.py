import sys, os
import fnmatch
from glob import glob
import unittest
from distutils.core import setup, Command

def get_relative_path():
    return os.path.dirname(os.path.abspath(os.path.join(os.getcwd(), sys.argv[0])))

def get_test_names(search_path,search_pattern,base_dir):
    excluded_files = []
    module_names = []

    for root, dirs, files in os.walk(search_path):
        for test_file in files:
            if fnmatch.fnmatch(test_file, search_pattern) and files not in excluded_files:
                filename = os.path.join(root, test_file)
                #Figure out the module name
                module_name = os.path.relpath(filename,base_dir).split('/')
                del module_name[-1] #remove filename from list
                module_name.append(os.path.splitext(test_file)[0])#add class name
                module_names.append('.'.join(module_name))
                
    return module_names

def create_test_suite(search_path, search_pattern, base_dir, reverse_order=True):
    TestSuite = unittest.TestSuite()
    module_names = get_test_names(search_path, search_pattern, base_dir)
    loadedTests = unittest.TestLoader().loadTestsFromNames(sorted(module_names, reverse=reverse_order))
    TestSuite.addTests(loadedTests)

    return TestSuite

def create_deployment_test_suite(insert_data):
    from dbsclient_t.deployment.DBSDeployment_t import PrepareDeploymentsTests
    from dbsclient_t.deployment.DBSDeployment_t import PostDeploymentTests
    
    RESTModel = ('DBSReader','DBSWriter')

    TestSuite = unittest.TestSuite()

    if insert_data:
        prepareTests = unittest.TestLoader().loadTestsFromTestCase(PrepareDeploymentsTests)
        TestSuite.addTests(prepareTests)
    
    for model in RESTModel:
        loadedTests = unittest.TestLoader().loadTestsFromTestCase(PostDeploymentTests)

        for test in loadedTests:
            test.RESTModel = model

        TestSuite.addTests(loadedTests)

    return TestSuite

class TestCommand(Command):

    user_options =[('unit=', None, 'Run client unittests options are ClientWriter, ClientReader, ClientBlockWriter'),
                   ('unitall', None, 'Run client unittests'),
                   ('validation', None, 'Run client validation tests'),
                   ('deployment', None, 'Run client deployment tests'),
                   ('insert', None, 'Insert data during deployment tests'),
                   ('cmsweb-testbed', None, 'Run standarized cmsweb-testbed validation tests'),
                   ('host=', None, 'Host to run unittests')]

    description = """Test DBS3 Client using provided unittests, possible options are\n
                  --unit=ClientWriter, --unittest=ClientReader or --unittest=ClientBlockWriter to run client unittests\n
                  --unitall to run writer, reader and bulk client unittests\n
                  --validation to run client validation tests\n
                  --deployment to run client deployment tests\n
                  --insert data during client deployment tests\n
                  --cmsweb-testbed to run standarized cmsweb-testbed validation tests\n
                  --host= to run unittests"""

    def initialize_options(self):
        self.unit = None
        self.unitall = None
        self.validation = None
        self.deployment = None
        self.insert = None
        self.cmsweb_testbed = None
        self.host = None

    def finalize_options(self):
        #Check if environment us set-up correctly
        if not os.environ.get("DBS3_CLIENT_ROOT"):
            print """You have to source init.sh before running unittests\n
            If you are using rpm based development environment on a VM, \n
            try to source /data/current/<Repository>/slc5_amd64_gcc461/cms/dbs3-client/<Version>/etc/profile.d/init.sh."""
            sys.exit(1)

        if not self.host:
            print "Please, specify a host to use, for example using --host=https://cmsweb-testbed.cern.ch"
            sys.exit(2)

        if not (self.unit or self.unitall or self.validation or self.deployment or self.cmsweb_testbed):
            print "Please, specify one of the following options.\n%s" % self.description
            sys.exit(3)

        if self.unit not in (None, 'ClientWriter', 'ClientReader', 'ClientBlockWriter'):
            print "Valid options for --unit are ClientWriter, ClientReader or ClientBlockWriter"
            sys.exit(4)

    def run(self):
        base_dir = get_relative_path()
        test_dir = os.path.join(base_dir, 'dbsclient_t')
        unit_tests = os.path.join(test_dir, 'unittests')
        validation_tests = os.path.join(test_dir, 'validation')
        deployment_tests = os.path.join(test_dir, 'deployment')

        TestSuite = unittest.TestSuite()

        if self.cmsweb_testbed:
            self.unitall, self.validation, self.deployment = (True, True, True)

        if self.unit in ('ClientWriter','ClientReader','ClientBlockWriter'):
            ###set environment
            os.environ['DBS_READER_URL'] = ("%s/dbs/dev/global/DBSReader") % self.host
            os.environ['DBS_WRITER_URL'] = ("%s/dbs/dev/global/DBSWriter") % self.host

            TestSuite.addTests(create_test_suite(unit_tests, 'DBS%s_t.py' % self.unit, base_dir))

        if self.unitall:
            ###set environment
            os.environ['DBS_READER_URL'] = ("%s/dbs/dev/global/DBSReader") % self.host
            os.environ['DBS_WRITER_URL'] = ("%s/dbs/dev/global/DBSWriter") % self.host
           
            TestSuite.addTests(create_test_suite(unit_tests, 'DBSClient*_t.py', base_dir))

        if self.validation:
            ###set environment
            os.environ['DBS_READER_URL'] = ("%s/dbs/dev/global/DBSReader") % self.host
            os.environ['DBS_WRITER_URL'] = ("%s/dbs/dev/global/DBSWriter") % self.host
           
            TestSuite.addTests(create_test_suite(validation_tests, 'DBSValidation_t.py', base_dir))

        if self.deployment:
            for instance in ('dev','int','prod'):
                ###set environment
                os.environ['DBS_READER_URL'] = ("%s/dbs/%s/global/DBSReader") % (self.host, instance)
                os.environ['DBS_WRITER_URL'] = ("%s/dbs/%s/global/DBSWriter") % (self.host, instance)
                
                TestSuite.addTests(create_deployment_test_suite(self.insert))

        unittest.TextTestRunner(verbosity=2).run(TestSuite)

setup(name = 'dbs',
      version = '3.0',
      maintainer_email = 'hn-cms-dmDevelopment@cern.ch',
      cmdclass = { 'test_system': TestCommand})


