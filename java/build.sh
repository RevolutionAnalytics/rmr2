echo "Note this assumes that the paths/versions are correct, make changes as necesssary"
HADOOP_PATH="/usr/lib/hadoop"
HBASE_PATH="/usr/lib/hbase"

echo "Copying HBASE libs to Hadoop library path (simple way so that it can find them)"
sudo cp -R ${HBASE_PATH}/lib/* ${HADOOP_PATH}/lib/
sudo cp -R ${HBASE_PATH}/*.jar ${HADOOP_PATH}/lib/

echo "Copying libs into local build directory"
cp ${HBASE_PATH}/lib/commons-logging* ./lib/
cp ${HBASE_PATH}/hbase-* ./lib/
cp ${HADOOP_PATH}/hadoop-*-core.jar ./lib/
cp ${HADOOP_PATH}/contrib/streaming/hadoop-streaming-*.jar ./lib/


echo "Building hadoopy_hbase.jar"
ant

echo "Copying hadoopy_hbase.jar into Hadoop library path"
cp build/dist/hadoopy_hbase.jar ${HADOOP_PATH}/lib/hadoopy_hbase.jar

echo "Restarting jobtracker and tasktracker"
/etc/init.d/hadoop-0.20-jobtracker restart
/etc/init.d/hadoop-0.20-tasktracker restart