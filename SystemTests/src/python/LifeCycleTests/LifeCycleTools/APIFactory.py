
from dbs.apis.dbsClient import DbsApi
from DataProvider.core.dbs_provider import DBSDataProvider
from DataProvider.core.phedex_provider import PhedexDataProvider

class DBS3ApiFactory(object):
    def __init__(self, config):
        self.config = config or {}

    def get_api(self):
        return DbsApi(url=self.config.get("url", "https://cmsweb.cern.ch/dbs/int/global/DBSReader/"))

class DBSDataProvider(object):
    def __init__(self, config):
        self.config = config or {}

    def get_api(self):
        return DBSDataProvider(**self.config)

class PhedexDataProvider(object):
    def __init__(self, config):
        self.config = config or {}

    def get_api(self):
        return PhedexDataProvider()

def create_api(api="DbsApi", config=None):
    known_factory = {'DbsApi': DBS3ApiFactory(config),
                     'DBSDataProvider': DBSDataProvider(config),
                     'PhedexDataProvider': PhedexDataProvider(config)}

    factory = known_factory.get(api, None)

    if not factory:
        raise NotImplementedError("A factory for api %s has not yet been implemented." % (api))

    return factory.get_api()

if __name__ == "__main__":
    api = create_api(api="DbsApi", config=dict(url="https://cmsweb.cern.ch/dbs/int/global/DBSReader/"))
    print(dir(api))
