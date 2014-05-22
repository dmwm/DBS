#!/usr/bin/env python
"""

 DBS 3 script to calculate accumulated events per day for a interval.

 python EventsPerDay.py -d '/*/*Fall13-POST*/GEN-SIM'

"""

import  sys,time
from optparse import OptionParser
from time import gmtime
from dbs.apis.dbsClient import DbsApi
from dbs.exceptions.dbsClientException import dbsClientException

def main():
    usage="%prog <options>"

    parser = OptionParser(usage=usage)
    parser.add_option("-u", "--url", dest="url", help="DBS Instance url. default is https://cmsweb.cern.ch/dbs/prod/global/DBSReader", metavar="<url>")
    parser.add_option("-l", "--length", dest="length", help="Number of days for calculate the accumated events. It is Optional, default is 30 days.", metavar="<length>")
    parser.add_option("-d", "--dataset", dest="dataset", help="The dataset name for cacluate the events. Can be optional if datatier is used.", metavar="<dataset>")
    parser.add_option("-t", "--datatier", dest="datatier", help="The datatier name for cacluate the events. Can be optional if dataset is used. In this version datatier is not supported yet.", metavar="<data_tier_name>")
    parser.add_option("-a", "--access_type", dest="ds_access_type", help="Dataset access types: VALID, PRODUCTION or ALL(VALID+PRODUCTION). Default is ALL", metavar="<dataset_access_type>")
   
    parser.set_defaults(url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader")
    parser.set_defaults(length=30)
    parser.set_defaults(ds_access_type="ALL")
   
    (opts, args) = parser.parse_args()
    if not (opts.dataset or opts.datatier):
	parser.print_help()
	parser.error('either --dataset or --datatier is required')	

    data = opts.dataset
    #seconds per day	
    sdays = 86400
    len = opts.length
    now = time.time() 
    #print now	
    then = now - sdays*len
    #print data
    url = opts.url
    api=DbsApi(url=url)
    outputDataSets = []
    if (opts.ds_access_type == "ALL"):
	outputDataSetsValid = api.listDatasets(dataset=data,detail=1, dataset_access_type="VALID")
	outputDataSetsProd = api.listDatasets(dataset=data,detail=1, dataset_access_type="PRODUCTION")
	outputDataSets = outputDataSetsValid + outputDataSetsProd
    elif (opts.ds_access_type == "VALID"):
	outputDataSets = api.listDatasets(dataset=data,detail=1, dataset_access_type="VALID")
    elif (opts.ds_access_type == "PRODUCTION"):
	outputDataSets = api.listDatasets(dataset=data,detail=1, dataset_access_type="PRODUCTION")		

    f = []

    for i in range(len):
       f.append(0)

    for dataset in outputDataSets:
       inp=dataset['dataset']
       ct = dataset['creation_date']
       if ct > (then-30*sdays):
           blocks = api.listBlocks(dataset=inp, detail=True)
           for block in blocks:
               reply= api.listBlockSummaries(block_name=block['block_name'])
               neventsb= reply[0]['num_event']
               reply=api.listFiles(block_name=block['block_name'],detail=True)
               ct=reply[0]['last_modification_date']
               for x in range (len):
                   tnow = now - (len-x)*86400
                   if ct > then and ct < tnow:
                       f[x]=f[x]+ neventsb
    for i in range(len):
       print i, ":  ", f[i] 
    sys.exit(0);

if __name__ == "__main__":
    main()
    sys.exit(0);
