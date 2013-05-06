import cherrypy

from WMCore.Configuration import Configuration
from WMCore.REST.Server import RestApi

from dbs.web.DBSHelloWorld import HelloWorld

class DBSRestApi(RESTApi):
    def __init__(self, app, config, mount):
        """
        :arg app: application object passed to all entities
        :arg config: configuration object passed to all entities
        :arg mount: API URL mount point passed to all entities
        """
        super(DBSRestApi, self).__init__(app, config, mount)

        cherrypy.log('Starting up DBSRestv2 test')

        self._add({"hello" : HelloWorld(self, app, config, mount)})
