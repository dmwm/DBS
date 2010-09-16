"""
Profile DBS3 Server to study performance of all methods 
from RESTApi to database backend.
"""

import os
import cProfile
from dbsserver_t.utils.DBSRestApi import DBSRestApi

CONFIG = os.environ["DBS_TEST_CONFIG_READER"]
API = DBSRestApi(CONFIG)

def profile():
    for i in range(100):
        result = API.list('files', dataset='/Monitor/Commissioning09-v1/RAW')
        #print result

if __name__ == "__main__":
    cProfile.run("profile()")
