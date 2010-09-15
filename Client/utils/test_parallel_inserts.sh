#!/bin/bash
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8585/dbs3 /GlobalAug07-C/Online/RAW > log1 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8585/dbs3 /GlobalAug07-B/Online/RAW  > log2 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8585/dbs3 /GlobalAug07-A/Online/RAW  > log3 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8585/dbs3 /GlobalNov07-Express/Online/RAW  > log4 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8585/dbs3 /Monitor/Commissioning08-v1/RAW  > log5 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8585/dbs3 /TestBeam2007-combined/Online/RAW  > log6 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8585/dbs3 /Test/Commissioning09-Express-v1/RAW  > log7 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8585/dbs3 /DaqTest-default/Online/RAW  > log8 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8585/dbs3 /TestBeam2007Ecal-A/Online/RAW  > log9 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8585/dbs3 /GlobalNov07-A/Online/RAW  > log10 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8585/dbs3 /DaqTest-C/Online/RAW  > log11 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8585/dbs3 /DaqTest-B/Online/RAW  > log12 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8585/dbs3 /MuonPrivateGlobal-A/Online/RAW  > log13 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8585/dbs3 /MuonPrivateGlobal-Express/Online/RAW  > log14 2>&1 &
python2.6 dbs2Todbs3DatasetMigrate.py http://vocms09.cern.ch:8585/dbs3 /DaqTest-A/Online/RAW  > log15 2>&1 &
wait
