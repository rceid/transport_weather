#!/usr/bin/env bash

year=2019
while [ $year -ge 1991 ]
do
	mkdir ./CSVs	
	curl http://s3-us-gov-west-1.amazonaws.com/cg-d4b776d0-d898-4153-90c8-8336f86bdfec/masters/nibrs/nibrs-$year.zip | zcat | ./txt_to_csv.py $year
	for csv in ./CSVs/*.csv; 
	do
		file="${csv:7}"
		folder="${file%%-*}"
		cat $csv | curl -x "socks5h://localhost:8157" -X PUT -T - "http://ip-172-31-0-119.us-east-2.compute.internal:9864/webhdfs/v1/tmp/reid7/$folder/$file?op=CREATE&user.name=hadoop&namenoderpcaddress=ip-172-31-11-144.us-east-2.compute.internal:8020&createflag=&createparent=true&overwrite=false"
	done
	rm -r ./CSVs
	(( year-- ))
done
