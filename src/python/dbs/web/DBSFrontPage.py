"""
DBS 3 Frontpage for RESTAPI
"""
import os, re
from WMCore.REST.Server import RESTFrontPage

class DBSFrontPage(RESTFrontPage):
    """
    DBS 3 Front Page
    """
    def __init__(self, app, config, mount):
        frontpage = "dbs/templates/dbs.html"

        file_location = os.path.abspath(__file__)
        position = file_location.find('/xlib/')
        position = (position >= 0 and position) or file_location.find('/lib/')
        rootdir = file_location[:position]
        X = (__file__.find("/xlib/") >= 0 and "x") or ""

        roots = {'dbs' : {'root' : "%s/%sdata/" % (rootdir, X),
                          'rx' : re.compile(r'^[a-z]+/[-a-z0-9]+\.(?:css|js|png|gif|html)$')
                         }
                }

        RESTFrontPage.__init__(self, app, config, mount, frontpage, roots)
