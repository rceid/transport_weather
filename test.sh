#!/usr/bin/env bash

year=2019
while [ $year -le 2019 ]
do
	cat testdata.txt | ./txt_to_csv.py $year

	for csv in ./CSVs/*.csv; do
		file="${csv:7}"
		folder="${file%%-*}"
		cat $csv | curl -x "socks5h://localhost:8157" -X PUT -T - "http://ip-172-31-0-119.us-east-2.compute.internal:9864/webhdfs/v1/tmp/reid7/$folder/$file?op=CREATE&user.name=hadoop&namenoderpcaddress=ip-172-31-11-144.us-east-2.compute.internal:8020&createflag=&createparent=true&overwrite=false"
	done
	rm -r ./CSVs
	(( year++ ))
done

