#!/usr/bin/env python
#
import os
import sys
import time
from threading import Thread
import xml.sax, xml.sax.handler

from DBSAPI.dbsApi import DbsApi as Dbs2Api
from DBS3SimpleClient import DBS3Client


class DBS2to3Migrate(Thread):
    def __init__(self, ic, url, dataset):
        Thread.__init__(self)
        self.ic = ic
        self.url = url
        self.dataset = dataset
        self.dbs3api = DBS3Client(url)
        self.api = Dbs2Api()

    def insert(self, dbs3api, data, totaltime=0):
        blockinfo = {}
        class Handler(xml.sax.handler.ContentHandler):
            def __init__(self):
                self.primary_dataset = ''
                self.processed_dataset = ''
                self.creation_date = ''
                self.created_by = ''
                self.block_name = ''
                self.dataset = ''
                self.prdsobj = {}
                self.block = {}
                self.files = []
                self.currfile = {}
                self.currfilelumis = []
                self.currfileparents = []

            def startElement(self, name, attrs):
                if name == 'primary_dataset':
                    self.primary_dataset = attrs.get('primary_name')
                    self.prdsobj = { "PRIMARY_DS_NAME" : str(self.primary_dataset),
                                    "PRIMARY_DS_TYPE": "test" }
                if name == 'processed_dataset':
                    self.dataset =	{ "IS_DATASET_VALID": 1, 
                                "PRIMARY_DS_NAME": self.primary_dataset, 
                                "PRIMARY_DS_TYPE": "test", 
                                "DATASET_TYPE":"PRODUCTION",
						        "GLOBAL_TAG": attrs.get('global_tag'),
                                "XTCROSSSECTION":123,
                                "PHYSICS_GROUP_NAME": "Tracker", 
						        "PROCESSING_VERSION" : "1",
						        "PROCESSED_DATASET_NAME": attrs.get('processed_datatset_name'), 
                                "ACQUISITION_ERA_NAME" : attrs.get('acquisition_era') 
						}
                    
                    self.processed_dataset = attrs.get('processed_datatset_name')

                if name == 'path':
                    self.data_tier = str(attrs.get('dataset_path')).split('/')[3]
                    self.path = attrs.get('dataset_path')
                    self.dataset["DATA_TIER_NAME"] = self.data_tier 
                    self.dataset["DATASET"] = self.path

                if name == 'block':
                    self.block = {"BLOCK_NAME":attrs.get('name'), 
                            "OPEN_FOR_WRITING":1,
                            "BLOCK_SIZE": attrs.get('size'), 
						    "FILE_COUNT":attrs.get('number_of_files'), 
                            "CREATION_DATE":attrs.get('creation_date'), 
						    "CREATE_BY":attrs.get('created_by'), 
                            "LAST_MODIFICATION_DATE":attrs.get('last_modification_date'), 
						    "LAST_MODIFIED_BY":attrs.get('last_modified_by')}
                    self.block_name = attrs.get('name')

                if name == 'storage_element':
                    self.block["ORIGIN_SITE"] = attrs.get('storage_element_name')

                if name == 'file':
                    self.currfile = {"LOGICAL_FILE_NAME":attrs.get('lfn'), 
                            "IS_FILE_VALID": 1, 
                            "DATASET": self.path, 
                            "BLOCK" : self.block_name,
					        "FILE_TYPE": "EDM",
					        "CHECK_SUM": attrs.get('checksum'), 
                            "EVENT_COUNT": attrs.get('number_of_events'), 
                            "FILE_SIZE": attrs.get('size'), 
					        "ADLER32": attrs.get('adler32'), 
                            "MD5": attrs.get('md5'), 
                            "AUTO_CROSS_SECTION": 0.0, 
					        "CREATE_BY":attrs.get('created_by'), 
                            "LAST_MODIFICATION_DATE":attrs.get('last_modification_date'),
                            "LAST_MODIFIED_BY":attrs.get('last_modified_by')}

                if name == 'file_lumi_section':
                    filelumi = {
						"RUN_NUM":attrs.get('run_number'),
						"LUMI_SECTION_NUM":attrs.get('lumi_section_number')}
                    self.currfilelumis.append(filelumi)
                    
                if name == 'file_parent':
                    fileparent = {"FILE_PARENT_LFN":attrs.get('lfn')}
                    self.currfileparents.append(fileparent)

            def endElement(self, name) :
                if name == 'file' : 
                    self.currfile["FILE_LUMI_LIST"] = self.currfilelumis
                    self.currfile["FILE_PARENT_LIST"] = self.currfileparents
                    self.currfilelumis = []
                    self.currfileparents = []
                    self.files.append(self.currfile)

                if name == 'dbs':
                    dbs3api.put("primarydatasets", self.prdsobj)
                    dbs3api.put("datasets", self.dataset)
                    dbs3api.put("blocks", self.block)
                    start_time = time.time()
                    dbs3api.put("files", {"files" : self.files})
                    end_time = time.time()
                    tm = end_time-start_time
                    nfiles = len(self.files)
                    nlumis = sum([len(f["FILE_LUMI_LIST"]) for f in self.files])
                    nparents = sum([len(f["FILE_PARENT_LIST"]) for f in self.files])
                    weight = nfiles * (nlumis+nparents+1)
                    print "BLOCK: %s,  WEIGHT: %s, TIME: %s sec" % (self.block["BLOCK_NAME"], weight, tm)

                    blockinfo["time"] = tm
                    blockinfo["weight"] = weight

        xml.sax.parseString(data, Handler())
        return blockinfo

    def run(self):
        dt = 0
        dw = 0
        blocks = self.api.listBlocks(self.dataset)
        for ablock in blocks:
            blockName = ablock["Name"]
            fileName = blockName.replace('/', '_').replace('#', '_') + ".xml"
            if os.path.exists(fileName):
                data = open(fileName, "r").read()
            else:	
                data = self.api.listDatasetContents(self.dataset, ablock["Name"])
                fp = open(fileName, "w")
                fp.write(data)
                fp.close()
            block = self.insert(self.dbs3api, data)
            dt += block["time"]
            dw += block["weight"] 
        print "DATASET: %s, WEIGHT: %s, TOTAL TIME: %s" % (self.dataset, dw+1, dt)

                
if __name__ == "__main__":
    DATASETS = open("datasets.txt").readlines()
    URL = {"java":"http://vocms09.cern.ch:8989/DBSServlet/",
           "xxpy":"http://localhost/intlxx/",
           "yypy":"http://localhost/intlyy/"}
    for i in range(len(DATASETS)):
        curr = DBS2to3Migrate(ic = i, url=URL[sys.argv[1]], dataset = DATASETS[i].strip())
        curr.start()

