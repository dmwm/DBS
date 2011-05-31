import sys, os, os.path, re, shutil, string
from distutils.core import setup, Command
from distutils.command.build import build
from distutils.command.install import install
from distutils.spawn import spawn
from glob import glob

systems = \
{
  'Client':
  {
    'srcdir': 'Client/src/python',
    'bin': ['Client/cmdline/dbs.py'],
    'pythonmods': ['dbs.__init__'],
    'pythonpkg': ['dbs.apis', 'dbs.exceptions'],
    'examples': ['Client/utils/*.py']
  },

  'Server':
  {
    'srcdir': 'Server/Python/src',
    'pythonmods': ['dbs.__init__',
                   'dbs.web.DBSReaderModel',
                   'dbs.web.DBSWriterModel'],
    'pythonpkg': ['dbs.business',
                  'dbs.dao',
                  'dbs.dao.Oracle',
                  'dbs.dao.Oracle.AcquisitionEra',
                  'dbs.dao.Oracle.ApplicationExecutable',
                  'dbs.dao.Oracle.AssociatedFile',
                  'dbs.dao.Oracle.Block',
                  'dbs.dao.Oracle.BlockParent',
                  'dbs.dao.Oracle.BlockSite',
                  'dbs.dao.Oracle.BlockStorageElement',
                  'dbs.dao.Oracle.BranchHashe',
                  'dbs.dao.Oracle.ComponentStatus',
                  'dbs.dao.Oracle.Dataset',
                  'dbs.dao.Oracle.DatasetOutputMod_config',
                  'dbs.dao.Oracle.DatasetParent',
                  'dbs.dao.Oracle.DatasetRun',
                  'dbs.dao.Oracle.DatasetType',
                  'dbs.dao.Oracle.DataTier',
                  'dbs.dao.Oracle.DbsVersion',
                  'dbs.dao.Oracle.DoNothing',
                  'dbs.dao.Oracle.File',
                  'dbs.dao.Oracle.FileBuffer',
                  'dbs.dao.Oracle.FileLumi',
                  'dbs.dao.Oracle.FileOutputMod_config',
                  'dbs.dao.Oracle.FileParent',
                  'dbs.dao.Oracle.FileParentBlock',
                  'dbs.dao.Oracle.FileType',
                  'dbs.dao.Oracle.InsertTable',
                  'dbs.dao.Oracle.MigrationBlock',
                  'dbs.dao.Oracle.MigrationRequests',
                  'dbs.dao.Oracle.OutputModuleConfig',
                  'dbs.dao.Oracle.ParameterSetHashe',
                  'dbs.dao.Oracle.PhysicsGroup',
                  'dbs.dao.Oracle.PrimaryDataset',
                  'dbs.dao.Oracle.PrimaryDSType',
                  'dbs.dao.Oracle.ProcessedDataset',
                  'dbs.dao.Oracle.ProcessingEra',
                  'dbs.dao.Oracle.ReleaseVersion',
                  'dbs.dao.Oracle.Service',
                  'dbs.dao.Oracle.Site',
                  'dbs.dao.Oracle.StorageElement',
                  'dbs.utils'
               ]
  }
}

def get_relative_path():
  return os.path.dirname(os.path.abspath(os.path.join(os.getcwd(), sys.argv[0])))

def define_the_build(self, dist, system_name, run_make = True, patch_x = ''):
  # Expand various sources.
  system = systems[system_name]
  exsrc = sum((glob("%s" % x) for x in system.get('examples', [])), [])
  binsrc = sum((glob("%s" % x) for x in system.get('bin', [])), [])

  # Specify what to install.
  py_version = (string.split(sys.version))[0]
  pylibdir = '%slib/python%s/site-packages' % (patch_x, py_version[0:3])
  dist.py_modules = system.get('pythonmods', [])
  dist.packages = system.get('pythonpkg', [])
  dist.package_dir = { '': system.get('srcdir', []) }
  dist.data_files = [('examples', exsrc), ('%sbin' % patch_x, binsrc)]

class BuildCommand(Command):
  """Build python modules for a specific system."""
  description = \
    "Build python modules for the specified system. Possible\n" + \
    "\t\t   systems are 'Server' or 'Client'.\n" + \
    "Use with --force to\n" + \
    "\t\t   ensure a clean build of only the requested parts.\n"
  user_options = build.user_options
  user_options.append(('system=', 's', 'build the specified system'))

  def initialize_options(self):
    self.system = None

  def finalize_options(self):
    # Check options.
    if self.system == None:
      print "System not specified, please use '-s Server' or '-s Client'"
      sys.exit(1)
    elif self.system not in systems:
      print "System %s unrecognised, please use '-s Server' or '-s Client'" % self.system
      sys.exit(1)

    # Expand various sources and maybe do the c++ build.
    define_the_build(self, self.distribution, self.system, True, '')

    # Force rebuild.
    shutil.rmtree("%s/build" % get_relative_path(), True)
     
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
    "Install a specific system, either 'Server' or 'Client'. You can\n" + \
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
      print "System not specified, please use '-s Server' or 'Client'"
      sys.exit(1)
    elif self.system not in systems:
      print "System %s unrecognised, please use '-s Server' or 'Client'" % self.system
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

setup(name = 'dbs',
      version = '3.0',
      maintainer_email = 'hn-cms-dmDevelopment@cern.ch',
      cmdclass = { 'build_system': BuildCommand,
                   'install_system': InstallCommand })
