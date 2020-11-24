#!/usr/bin/env bash

for file in ./weather/*
do
	fname="${file:10}"
	$file | curl -x "socks5h://localhost:8157" -X PUT -T - "http://ip-172-31-0-119.us-east-2.compute.internal:9864/webhdfs/v1/tmp/reid7/weather/13-14?op=CREATE&user.name=hadoop&namenoderpcaddress=ip-172-31-11-144.us-east-2.compute.internal:8020&createflag=&createparent=true&overwrite=false"
done


