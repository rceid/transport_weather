# Untars NOAA weather data tarballs
# Note. I already ran on cluster. Please don't run again
for f in *.tar;
do
  tar xf $f
  rm $f
done
