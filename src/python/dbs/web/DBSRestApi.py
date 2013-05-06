import cherrypy

from WMCore.Configuration import Configuration
from WMCore.REST.Server import RESTApi

from dbs.web.DBSHelloWorld import HelloWorld

class DBSRestApi(RESTApi):
    def __init__(self, app, config, mount):
        """
        :arg app: application object passed to all entities
        :arg config: configuration object passed to all entities
        :arg mount: API URL mount point passed to all entities
        """
        ### MiniRESTApi is not a new style class, super does not work
        RESTApi.__init__(self, app, config, mount)

        cherrypy.log('Starting up DBSRestv2 test')

        self._add({"hello" : HelloWorld(self, app, config, mount)})
