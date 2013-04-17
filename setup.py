import sys, os, os.path, re, shutil, string
from distutils.core import setup, Command
from distutils.command.build import build
from distutils.command.install import install
from distutils.spawn import spawn
from glob import glob

systems = \
{
  'LifeCycleTests':
  {
    'srcdir': 'src/python',
    'bin': ['bin/dbs3BulkInsert.py',
            'bin/dbs3CrabWorkflow.py',
            'bin/dbs3Crab3Workflow.py',
            'bin/dbs3dasComparision.py',
            'bin/das_logfile_analyser.py',
            'bin/das_logfile_parser.py',
            'bin/dbs3DASAccess.py',
            'bin/dbs3DASGetQueries.py',
            'bin/dbs3IntroduceFailures.py',
            'bin/dbs3GetBlocks.py',
            'bin/dbs3GetDatasets.py',
            'bin/dbs3GetFileLumis.py',
            'bin/dbs3GetFileParents.py',
            'bin/dbs3GetFiles.py',
            'bin/dbs3GetPrimaryDSType.py',
            'bin/dbs3WriterStressTest.py',
            'bin/getFakeData.py',
            'bin/StatsServer.py',
            'bin/run_job_wn.sh',
            'bin/submit_jobs.sh'],
    'pythonmods': ['LifeCycleTests.__init__'],
    'pythonpkg': ['LifeCycleTests.LifeCycleTools'],
    'conf' : ['DBS3AnalysisLifecycle.conf',
              'DBS3BulkInsertLifecycle.conf',
              'PhedexDBSDASLifecylce.conf'],
    'data' : ['dbs_queries_20120828.json']
  },

  'LifeCycleAnalysis':
  {
    'srcdir': 'src/python',
    'bin': ['bin/LifeCyclePlots.py',
            'bin/MergeDB.sh'],
    'pythonmods': ['LifeCycleAnalysis.__init__'],
    'pythonpkg': ['LifeCycleAnalysis.LifeCyclePlots']
  }
}

def get_relative_path():
  return os.path.dirname(os.path.abspath(os.path.join(os.getcwd(), sys.argv[0])))

def define_the_build(self, dist, system_name, run_make = True, patch_x = ''):
  # Expand various sources.
  docroot = "doc/build/html"
  system = systems[system_name]
  confsrc = sum((glob("conf/%s" % x) for x in system.get('conf',[])), [])
  datasrc = sum((glob("data/%s" % x) for x in system.get('data',[])), [])
  binsrc = sum((glob("%s" % x) for x in system.get('bin', [])), [])

  # Specify what to install.
  py_version = (string.split(sys.version))[0]
  pylibdir = '%slib/python%s/site-packages' % (patch_x, py_version[0:3])
  dist.py_modules = system.get('pythonmods', [])
  dist.packages = system.get('pythonpkg', [])
  dist.package_dir = { '': system.get('srcdir', []) }
  dist.data_files = [('%sbin' % patch_x, binsrc), ('conf', confsrc), ('data', datasrc)]
  if os.path.exists(docroot):
    for dirpath, dirs, files in os.walk(docroot):
      dist.data_files.append(("%sdoc%s" % (patch_x, dirpath[len(docroot):]),
                              ["%s/%s" % (dirpath, fname) for fname in files
                               if fname != '.buildinfo']))

class BuildCommand(Command):
  """Build python modules for a specific system."""
  description = \
    "Build python modules for the specified system. Possible\n" + \
    "\t\t   systems are 'LifeCycleTests'.\n" + \
    "Use with --force to\n" + \
    "\t\t   ensure a clean build of only the requested parts.\n"
  user_options = build.user_options
  user_options.append(('system=', 's', 'build the specified system'))

  def initialize_options(self):
    self.system = None

  def finalize_options(self):
    # Check options.
    if self.system == None:
      print "System not specified, please use '-s LifeCycleTests or -s LifeCycleAnalysis'"
      sys.exit(1)
    elif self.system not in systems:
      print "System %s unrecognised, please use '-s LifeCycleTests or -s LifeCycleAnalysis'" % self.system
      sys.exit(1)

    # Expand various sources and maybe do the c++ build.
    define_the_build(self, self.distribution, self.system, True, '')

    # Force rebuild.
    shutil.rmtree("%s/build" % get_relative_path(), True)
    shutil.rmtree("doc/build", True)

  def run(self):
    command = 'build'
    if self.distribution.have_run.get(command): return
    cmd = self.distribution.get_command_obj(command)
    cmd.force = self.force
    cmd.ensure_finalized()
    cmd.run()
    self.distribution.have_run[command] = 1

class InstallCommand(install):
  """Install a specific system."""
  description = \
    "Install a specific system, 'LifeCycleTests' or 'LifeCycleAnalysis'. You can\n" + \
    "\t\t   patch an existing installation instead of normal full installation\n" + \
    "\t\t   using the '-p' option.\n"
  user_options = install.user_options
  user_options.append(('system=', 's', 'install the specified system'))
  user_options.append(('patch', None, 'patch an existing installation'))

  def initialize_options(self):
    install.initialize_options(self)
    self.system = None
    self.patch = None

  def finalize_options(self):
    # Check options.
    if self.system == None:
      print "System not specified, please use '-s LifeCycleTests'"
      sys.exit(1)
    elif self.system not in systems:
      print "System %s unrecognised, please use '-s LifeCycleTests'" % self.system
      sys.exit(1)
    if self.patch and not os.path.isdir("%s/xbin" % self.prefix):
      print "Patch destination %s does not look like a valid location." % self.prefix
      sys.exit(1)

    # Expand various sources, but don't build anything from c++ now.
    define_the_build(self, self.distribution, self.system,
                     False, (self.patch and 'x') or '')

    # Whack the metadata name.
    self.distribution.metadata.name = self.system
    assert self.distribution.get_name() == self.system

    # Pass to base class.
    install.finalize_options(self)

    # Mangle paths if we are patching. Most of the mangling occurs
    # already in define_the_build(), but we need to fix up others.
    if self.patch:
      self.install_lib = re.sub(r'(.*)/lib/python(.*)', r'\1/xlib/python\2', self.install_lib)
      self.install_scripts = re.sub(r'(.*)/bin$', r'\1/xbin', self.install_scripts)

  def run(self):
    for cmd_name in self.get_sub_commands():
      cmd = self.distribution.get_command_obj(cmd_name)
      cmd.distribution = self.distribution
      if cmd_name == 'install_data':
        cmd.install_dir = self.prefix
      else:
        cmd.install_dir = self.install_lib
      cmd.ensure_finalized()

      self.run_command(cmd_name)
      self.distribution.have_run[cmd_name] = 1

setup(name = 'dbs3-lifecycletests',
      version = '0.1',
      maintainer_email = 'hn-cms-dmDevelopment@cern.ch',
      cmdclass = { 'build_system': BuildCommand,
                   'install_system': InstallCommand })
