#!/usr/bin/env python

""" DAO Object for Services table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):
    """Service Insert DAO Class."""

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = """INSERT INTO %sSERVICES (SERVICE_ID, NAME, TYPE, LOCATION, STATUS, ADMIN, URI, DB, VERSION, LAST_CONTACT, ALIAS, COMMENTS) 
		    VALUES (:service_id, :name, :type, :location, :status, :admin, :uri, :db, :version, :last_contact, :alias, :comments) """ % self.owner

    def execute(self, conn, daoinput, transaction = False):
	self.dbi.processData(self.sql, daoinput, conn, transaction)

