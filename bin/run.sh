#!/bin/bash

source /etc/profile;
home=`dirname $0`
cd $home

project=$1
if [ ! -n "$1" ] ;then
	echo "no project"
	exit 1
fi

if [ ! -n "$2" ] ;then
    #hive 表的dt
    dt=`date +%Y-%m-%d -d "-1 days"`
else
    dt=$2
fi

python run_import_db.py ${project} ${dt} &>>${project}_stdout 
