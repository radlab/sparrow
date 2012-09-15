FILES=/opt/tpch_hive/*
OUT_DIR=/tmp

for f in $FILES
do
  echo "Processing $f"
  qname=`echo $f |  sed "s/.*\(q[0-9]*\).*/\1/"`
  results_file=$OUT_DIR/$qname.results
  echo "" > $results_file
  for t in {1..10}
  do
    /root/shark/bin/shark-withinfo -f $f 2>&1 #|grep wait >> $results_file
  done
done
