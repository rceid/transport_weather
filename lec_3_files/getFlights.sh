# Note. I already ran on cluster. Please don't run again
for yr in `seq 2000 2019`; do
  for mnth in `seq 1 12`; do
    wget https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${yr}_${mnth}.zip
  done
done
