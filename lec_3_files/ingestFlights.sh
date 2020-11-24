# Unzips BTS flight data into HDFS
# Note. I already ran on cluster. Please don't run again
for name in *.zip
  do unzip -p $name | tail -n +2 | hdfs dfs -put - /inputs/airline/${name%.zip}.csv;
done
