#!/bin/sh
echo "****list****"
ab "http://cmssrv48.fnal.gov:8585/DBS/filelumis?logical_file_name=/store/relval/CMSSW_3_1_0_pre9/RelValWjet_Pt_80_120/GEN-SIM-DIGI-RAW-HLTDEBUG/IDEAL_31X_v1/0007/D65FC820-D54E-DE11-8E65-001617C3B6FE.root" >> abLogs/ab.log 2>&1
#
echo "****list****"
ab "http://cmssrv48.fnal.gov:8585/DBS/runs?minrun=1657&maxrun=1000000" >> abLogs/ab.log 2>&1
#
echo "****list dataset by parent dataset name****"
ab "http://cmssrv48.fnal.gov:8585/DBS/datasets?parent_dataset=/zz1j-alpgen/CMSSW_1_4_9-CSA07-4129/GEN-SIM" >> abLogs/ab.log 2>&1
#
echo "****list blocks by dataset name****"
ab "http://cmssrv48.fnal.gov:8585/DBS/blocks?dataset=/zz2j-alpgen/CMSSW_1_6_7-CSA07-1205616825/GEN-SIM-DIGI-RAW" >> abLogs/ab.log 2>&1
#
echo "****list blocks by site_name and dataset name****"
ab "http://cmssrv48.fnal.gov:8585/DBS/blocks?site_name=ccsrmt2.in2p3.fr&dataset=/RelValProdTTbar/CMSSW_3_3_1-MC_31X_V9_StreamTkAlJpsiMuMu-v1/ALCARECO " >> abLogs/ab.log 2>&1
#
echo "****list files by dataset name****"
ab "http://cmssrv48.fnal.gov:8585/DBS/files?dataset=/RelValProdTTbar/CMSSW_3_3_1-MC_31X_V9_StreamTkAlJpsiMuMu-v1/ALCARECO" >> abLogs/ab.log 2>&1
#
echo "****list files ****"
ab "http://cmssrv48.fnal.gov:8585/DBS/files?dataset=/RelValProdTTbar/CMSSW_3_3_1-MC_31X_V9_StreamTkAlJpsiMuMu-v2/ALCARECO&release_version=*&pset_hash=*&app_name=*" >> abLogs/ab.log 2>&1
#
echo "****list****"
ab "http://cmssrv48.fnal.gov:8585/DBS/files?block_name=/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RAW%233eb8529f-0f52-40f5-8e38-95a9dd82cd55" >> abLogs/ab.log 2>&1
#
echo "****list****"
ab "http://cmssrv48.fnal.gov:8585/DBS/outputconfigs?dataset=/RelValProdTTbar/CMSSW_3_3_1-MC_31X_V9-v1/GEN-SIM-RAW&logical_file_name=/store/relval/CMSSW_3_3_1/RelValProdTTbar/GEN-SIM-RAW/MC_31X_V9-v1/0002/B0B80EC8-E8BF-DE11-927D-000423D94534.root" >> abLogs/ab.log 2>&1
#
echo "****list dataset by release version ****"
ab "http://cmssrv48.fnal.gov:8585/DBS/datasets?release_version=CMSSW_1_6_0" >> abLogs/ab.log 2>&1
#
echo "****list****"
ab "http://cmssrv48.fnal.gov:8585/DBS/outputconfigs?dataset=/RelValProdTTbar/CMSSW_3_3_1-MC_31X_V9-v1/GEN-SIM-RAW&logical_file_name=/store/relval/CMSSW_3_3_1/RelValProdTTbar/GEN-SIM-RAW/MC_31X_V9-v1/0002/B0B80EC8-E8BF-DE11-927D-000423D94534.root&release_version=*&pset_hash=*" >> abLogs/ab.log 2>&1
#
echo "****list dataset by pset_hash abd app_name****"
ab "http://cmssrv48.fnal.gov:8585/DBS/datasets?pset_hash=7b2b44ebe0424b2f64c3d6aa86dc6b2a&app_name=cmsRun" >> abLogs/ab.log 2>&1
#
echo "****list****"
ab "http://cmssrv48.fnal.gov:8585/DBS/outputconfigs?dataset=/RelValProdTTbar/CMSSW_3_3_1-MC_31X_V9-v1/GEN-SIM-RAW&logical_file_name=/store/relval/CMSSW_3_3_1/RelValProdTTbar/GEN-SIM-RAW/MC_31X_V9-v1/0002/B0B80EC8-E8BF-DE11-927D-000423D94534.root&release_version=*&pset_hash=*&app_name=*" >> abLogs/ab.log 2>&1
#
echo "****list dataset by pset_hash, app_name and output_module_label****"
ab "http://cmssrv48.fnal.gov:8585/DBS/datasets?pset_hash=113d2aac5b6e5d7b921aca167033ba34&app_name=cmsRun&output_module_label=hltPoolOutput" >> abLogs/ab.log 2>&1
#
echo "****list****"
ab "http://cmssrv48.fnal.gov:8585/DBS/runs?logical_file_name=/store/mc/Summer08/Ztautau_M20/GEN-SIM-RAW/IDEAL_v9_v1/0010/C89A1A8A-4AD8-DD11-AE72-00D0680BF885.root" >> abLogs/ab.log 2>&1
#
echo "****list****"
ab "http://cmssrv48.fnal.gov:8585/DBS/outputconfigs?dataset=/z5j_800ptz1600_alpgen-alpgen/CMSSW_1_6_7-HLT-1203578129/GEN-SIM-DIGI-RECO&output_module_label=* " >> abLogs/ab.log 2>&1
#
echo "****list****"
ab "http://cmssrv48.fnal.gov:8585/DBS/fileparents?logical_file_name=/store/relval/CMSSW_3_3_1/RelValProdTTbar/GEN-SIM-RECO/MC_31X_V9-v2/0004/2C73E289-77BF-DE11-8E9F-002354EF3BE3.root">>abLogs/ab.log 2>&1
#
echo "****list****"
ab "http://cmssrv48.fnal.gov:8585/DBS/filelumis?block_name=/RelValTTbar/CMSSW_3_3_0_pre3-STARTUP31X_V7_StreamHcalCalDijets-v1/ALCARECO#eea18f40-00d0-4d13-b4b1-45010e2eccbf" >> abLogs/ab.log 2>&1
#
echo "****list****"
ab "http://cmssrv48.fnal.gov:8585/DBS/runs?dataset=/JetMonitor/IansMagicMushroomSoup-T0Test-AnalyzeThisAndGetAFreePhD-v10_2_pre14replaythingy_v6/RAW" >> abLogs/ab.log 2>&1
#
echo "****list****"
ab "http://cmssrv48.fnal.gov:8585/DBS/runs?block_name=/JetMonitor/IansMagicMushroomSoup-T0Test-AnalyzeThisAndGetAFreePhD-v10_2_pre14replaythingy_v6/RAW%235a1a58cd-67a1-4b95-a1d7-757fcfad9b95" >> abLogs/ab.log 2>&1
#
echo "****list****"
ab "http://cmssrv48.fnal.gov:8585/DBS/runs?minrun=5000" >> abLogs/ab.log 2>&1
#
echo "****list dataset by dataset name****"
ab "http://cmssrv48.fnal.gov:8585/DBS/datasets?dataset=/zz1j-alpgen/CMSSW_1_4_9-CSA07-4129/GEN-SIM" >> abLogs/ab.log 2>&1
#
echo "****list****"
ab "http://cmssrv48.fnal.gov:8585/DBS/outputconfigs?dataset=/RelValTTbar/CMSSW_3_3_0_pre6-MC_31X_V9_StreamMuAlOverlaps-v1/ALCARECO" >> abLogs/ab.log 2>&1
#
echo "****Done****"
