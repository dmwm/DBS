from WMCore.DAOFactory import DAOFactory

class listPrimaryDatasets

    def listPrimaryDatasets(self, input):
	self.daofactory = DAOFactory(package='dbs.dao', logger=self.logger, dbinterface=self.dbi)

	print "HELLO world"

        if input.has_key("primdsname"):
            primdsname=input["primdsname"]
            primdsname=primdsname.replace("*","%")
        else:
            primdsname=""

        api=self.daofactory(classname="dbsApi")
        result=api.listPrimaryDatasets(primdsname, "")
        data={'server_method':'listPrimaryDatasets'}
        data.update({'status': 'success', 'result':result})
        return data


