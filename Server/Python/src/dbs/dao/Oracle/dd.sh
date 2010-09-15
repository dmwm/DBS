ll=`ls -1`
for l in $ll; do
	cd /home/anzar/devDBS3/DBS/DBS3/Server/Python/src/dbs/dao/Oracle/$l
	echo $PWD
	cvs add *.py
	cd /home/anzar/devDBS3/DBS/DBS3/Server/Python/src/dbs/dao/Oracle
done

