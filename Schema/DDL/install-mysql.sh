#!/bin/bash
#
#create the database
$MYSQL_ROOT/bin/mysql_install_db --datadir=$MYSQL_PATH --port=$MYSQL_PORT --socket=$MYSQL_SOCK
#start the server
$MYSQL_ROOT/bin/mysqld_safe --datadir=$MYSQL_PATH --port=$MYSQL_PORT     --socket=$MYSQL_SOCK
--log-error=$MYSQL_ERR --pid-file=$MYSQL_PID --max_allowed_packet=32M &

#create root account: modify the root pd as what you want and the username/pd. change the hostname too
$MYSQL_ROOT/bin/mysqladmin --port=$MYSQL_PORT --socket=$MYSQL_SOCK -u root password "pd"
$MYSQL_ROOT/bin/mysqladmin --port=$MYSQL_PORT --socket=$MYSQL_SOCK -u root -h hostname password "pd"

#create dbs account
$MYSQL_ROOT/bin/mysql --socket=$MYSQL_SOCK -uroot -ppd mysql -e "CREATE USER username@localhost IDENTIFIED BY
'upd';"
$MYSQL_ROOT/bin/mysql --socket=$MYSQL_SOCK -uroot -ppd mysql -e "UPDATE user set
Select_priv='Y',Insert_priv='Y',Update_priv='Y',Delete_priv='Y',Create_priv='Y',Drop_priv='Y',References_priv='Y',Index_priv='Y',Alter_priv='Y',Create_tmp_table_priv='Y',Lock_tables_priv='Y',Execute_priv='Y',Create_view_priv='Y',Show_view_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y' where User='username';"

$MYSQL_ROOT/bin/mysql --socket=$MYSQL_SOCK --port=$MYSQL_PORT -uroot -ppd mysql -e "GRANT ALL ON CMS_DBS3.* TO dbs@localhost;"

