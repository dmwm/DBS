#!/bin/bash
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8989/DBSServlet /DaqTest-default/Online/RAW  > logs/log8J 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8989/DBSServlet /TestBeam2007Ecal-A/Online/RAW  > logs/log9J 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8989/DBSServlet /GlobalNov07-A/Online/RAW  > logs/log10J 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8989/DBSServlet /DaqTest-C/Online/RAW  > logs/log11J 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8989/DBSServlet /DaqTest-B/Online/RAW  > logs/log12J 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8989/DBSServlet /MuonPrivateGlobal-A/Online/RAW  > logs/log13J 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8989/DBSServlet /MuonPrivateGlobal-Express/Online/RAW  > logs/log14J 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8989/DBSServlet /DaqTest-A/Online/RAW  > logs/log15J 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8989/DBSServlet /GlobalAug07-C/Online/RAW > logs/log1J 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8989/DBSServlet /GlobalAug07-B/Online/RAW  > logs/log2J 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8989/DBSServlet /GlobalAug07-A/Online/RAW  > logs/log3J 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8989/DBSServlet /GlobalNov07-Express/Online/RAW  > logs/log4J 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8989/DBSServlet /Monitor/Commissioning08-v1/RAW  > logs/log5J 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8989/DBSServlet /TestBeam2007-combined/Online/RAW  > logs/log6J 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8989/DBSServlet /Test/Commissioning09-Express-v1/RAW  > logs/log7J 2>&1 &
wait
