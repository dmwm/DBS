import cherrypy

from WMCore.Configuration import Configuration
from WMCore.REST.Server import DatabaseRESTApi

from dbs.web.DBSHelloWorld import HelloWorld
from dbs.web.DBSDatasets import Datasets
from dbs.web.DBSFiles import Files

class DBSRestApi(DatabaseRESTApi):
    def __init__(self, app, config, mount):
        """
        :arg app: application object passed to all entities
        :arg config: configuration object passed to all entities
        :arg mount: API URL mount point passed to all entities
        """
        ### MiniRESTApi is not a new style class, super does not work
        DatabaseRESTApi.__init__(self, app, config, mount)

        cherrypy.log('Starting up DBSRestv2 test')

        self._add({"hello" : HelloWorld(app, self, config, mount),
                   "datasets" : Datasets(app, self, config, mount),
                   "files" : Files(app, self, config, mount)})
