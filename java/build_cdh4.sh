
#echo "Copying HBASE libs to Hadoop library path (simple way so that it can find them)"
#sudo cp -R ${HBASE_PATH}/lib/* ${HADOOP_HOME}/lib/
#sudo cp -R ${HBASE_PATH}/*.jar ${HADOOP_HOME}/lib/

echo "Copying libs into local build directory"
mkdir -p ./lib/
echo $HBASE_HOME
echo $HADOOP_HOME
cp ${HBASE_HOME}/lib/commons-logging* ./lib/
cp ${HBASE_HOME}/hbase-* ./lib/
cp ${HADOOP_COMMONS_HOME}/*.jar ./lib/
cp ${HADOOP_HOME}/hadoop-*-core.jar ./lib/
cp ${HADOOP_HOME}/contrib/streaming/hadoop-streaming-*.jar ./lib/
cp /usr/share/java/commons-codec.jar ./lib/


echo "Building hadoopy_hbase.jar"
ant

echo "Copying hadoopy_hbase.jar into Hadoop library path"
#cp build/dist/hadoopy_hbase.jar ${HADOOP_HOME}/lib/hadoopy_hbase.jar

#echo "Restarting jobtracker and tasktracker"
#/etc/init.d/hadoop-0.20-jobtracker restart
#/etc/init.d/hadoop-0.20-tasktracker restart