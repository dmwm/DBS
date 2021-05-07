#!/usr/bin/env python
"""            
               
 DBS 3 script to calculate accumulated events per day for a interval.
                   
 python EventsPerDay.py -d '/*/*Fall13-POST*/GEN-SIM'
                       
""" 

import  sys, time
from optparse import OptionParser
from time import gmtime
from dbs.apis.dbsClient import DbsApi
from dbs.exceptions.dbsClientException import dbsClientException

def main():

	usage="%prog <options>"

	parser = OptionParser(usage=usage)
	parser.add_option("-u", "--url", dest="url", help="DBS Instance url. default is https://cmsweb.cern.ch:8443/dbs/prod/global/DBSReader", metavar="<url>")
	parser.add_option("-l", "--length", dest="length", help="Number of days for calculate the accumated events. It is Optional, default is 30 days.", metavar="<length>")
	parser.add_option("-d", "--dataset", dest="dataset", help="The dataset name for cacluate the events. Can be optional if datatier is used.", metavar="<dataset>")
	parser.add_option("-t", "--datatier", dest="datatier", help="The datatier name for cacluate the events. Can be optional if dataset is used. In this version datatier is not supported yet.", metavar="<data_tier_name>")
	parser.add_option("-a", "--access_type", dest="ds_access_type", help="Dataset access types: VALID, PRODUCTION or ALL(VALID+PRODUCTION). Default is ALL", metavar="<dataset_access_type>")
	parser.set_defaults(url="https://cmsweb.cern.ch:8443/dbs/prod/global/DBSReader")
	parser.set_defaults(length=30)
	parser.set_defaults(ds_access_type="ALL")

	(opts, args) = parser.parse_args()
	if not (opts.dataset or opts.datatier):
		parser.print_help()
		parser.error('either --dataset or --datatier is required')

	dataset	 = opts.dataset
	#seconds per day    
	sdays = 86400
	lenth = int(opts.length)
	now = time.time()
	#now = 1391353032
	then = now - sdays*lenth
	url = opts.url
	api=DbsApi(url=url)
	outputDataSets = []
    
	f = [0 for x in range(lenth)]
	min_cdate = int(then)
	max_cdate = int(now)
	if (opts.ds_access_type == "ALL"):
		outputDataSetsValid = api.listDatasets(dataset=dataset, min_cdate=min_cdate-30*sdays, 
                          max_cdate=max_cdate, dataset_access_type="VALID")
		outputDataSetsProd = api.listDatasets(dataset=dataset, min_cdate=min_cdate-30*sdays,
                          max_cdate=max_cdate, dataset_access_type="PRODUCTION")
		outputDataSets = outputDataSetsValid + outputDataSetsProd
	elif (opts.ds_access_type == "VALID"):
		outputDataSets = api.listDatasets(dataset=dataset, min_cdate=min_cdate-30*sdays,
                          max_cdate=max_cdate, dataset_access_type="VALID")
	elif (opts.ds_access_type == "PRODUCTION"):
		outputDataSets = api.listDatasets(dataset=dataset, min_cdate=min_cdate-30*sdays,
                          max_cdate=max_cdate, dataset_access_type="PRODUCTION")
	for dataset in outputDataSets:
		outputBlocks = api.listBlocks(dataset=dataset["dataset"], detail=1, min_cdate=min_cdate, max_cdate=max_cdate)
		blockList = []
		blockCdate = {}
		for block in outputBlocks:
			blockList.append(block["block_name"])
			blockCdate[block["block_name"]] = block["creation_date"]
		blockSum = []
		if blockList: 
			blockSum = api.listBlockSummaries(block_name=blockList, detail=1)
		for b in blockSum:
			cdate= blockCdate[b["block_name"]]
			day = int((now-cdate)/sdays)
			f[day] = f[day] + b["num_event"] 
	for i in range(lenth):
		#print (lenth-1)-i, ":  ", f[i], "  ", sum(item['all'] for item in f[i:lenth]) 
		print(i, ": ", f[(lenth-1)-i], " ", sum(item for item in f[(lenth-1)-i:lenth]))
	sys.exit(0);

if __name__ == "__main__":
	main()
	sys.exit(0);
