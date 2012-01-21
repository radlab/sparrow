# Helper for building thrift files

for FILE in $(ls |grep thrift)
do 
  thrift --gen java:java5 -o ../ $FILE
done
