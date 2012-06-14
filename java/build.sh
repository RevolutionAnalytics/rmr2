ant
cp build/dist/hadoopy_hbase.jar /usr/lib/hadoop/lib/hadoopy_hbase.jar
/etc/init.d/hadoop-0.20-jobtracker restart
/etc/init.d/hadoop-0.20-tasktracker restart

