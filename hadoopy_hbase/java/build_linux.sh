#!/bin/bash
  
#Copyright 2013 Revolution Analytics
#   
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#     http:#www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS, 
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.


ARGS=$(getopt -o h -l "hadoop_path:,hbase_path:" -n "build_linux.sh" -- "$@");
eval set -- "$ARGS";

while true; do
  case "$1" in
    -h)
      shift;
      echo "  Usage:"
      echo "    $0 [--hadoop_path] [--hbase_path]"
      echo ""
      echo "  Optional arguments:"
      echo "    --hadoop_path    Path of the installed Hadoop"
      echo "    --hbase_path     Path of the installed HBase"
      echo ""
      echo "  Description: "
      echo "    The script builds the jar for hbase streaming. Dependent jars are"
      echo "    copied from the place specified via the '--haodop_path' and"
      echo "    '--hbase_path' options. If any of the paths is not specified, the"
      echo "    script trys its best to guess its correct location."
      echo ""
      echo "    Dependent jars are copied to ./lib, then 'ant' is used to build the target jar."
      echo ""
      echo "    If 'ant' cannot be found, the script will download ant's binary automatically."
      echo ""
      exit 0
      ;;
    --hadoop_path)
      shift;
      if [ -n "$1" ]; then
        declare -r hadoop_path=$1
        shift;
      fi
      ;;
    --hbase_path)
      shift;
      if [ -n "$1" ]; then
        declare -r hbase_path=$1
        shift;
      fi
      ;;
    --)
      shift;
      break;
      ;;
  esac
done

function discover_hadoop_path {
    local candidates

    # use HADOOP_HOME if it is defined
    if [ $HADOOP_HOME ]; then
        hadoop_path=$HADOOP_HOME
        return 0
    fi

    # use hadoop.home.dir if a jobtracker or tasktracker is running
    candidates=`ps aux \
        | grep -i 'jobtracker\|tasktracker' \
        | sed -rn  '/hadoop\.home\.dir/s/.*hadoop\.home\.dir=([^\ ]*) .*/\1/gp' \
        | uniq`

    for entry in $candidates ; do
        if [ -e ${entry}"/bin/hadoop" ]; then
            hadoop_path=$entry
            return 0
        fi
    done

    # try the best guesses
    for entry in '/usr/lib/hadoop' \
        '/usr/local/hadoop' ; do
        if [ -e ${entry}"/bin/hadoop" ]; then
            hadoop_path=$entry
            return 0
        fi
    done

    # search globally
    candidates=`find / -type d -name hadoop`
    for entry in $candidates ; do
        if [ -e ${entry}"/bin/hadoop" ]; then
            hadoop_path=$entry
            return 0
        fi
    done
}

function discover_hbase_path {
    local candidates

    # use HBASE_HOME if it is defined
    if [ $HBASE_HOME ]; then
        hbase_path=$HBASE_HOME
        return 0
    fi

    # use hadoop.home.dir if a jobtracker or tasktracker is running
    candidates=`ps aux \
        | grep -i 'hbase-master\|hbase-regionserver' \
        | sed -rn  '/hbase\.home\.dir/s/.*hbase\.home\.dir=([^\ ]*) .*/\1/gp' \
        | uniq`

    for entry in $candidates ; do
        if [ -e ${entry}"/bin/hbase" ]; then
            hbase_path=$entry
            return 0
        fi
    done

    # try the best guesses
    for entry in '/usr/lib/hbase' \
        '/usr/local/hbase' ; do
        if [ -e ${entry}"/bin/hbase" ]; then
            hbase_path=$entry
            return 0
        fi
    done

    # search globally
    candidates=`find / -type d -name hbase`
    for entry in $candidates ; do
        if [ -e ${entry}"/bin/hbase" ]; then
            hbase_path=$entry
            return 0
        fi
    done

}

if [ ! $hadoop_path ]; then
    declare -x hadoop_path
    discover_hadoop_path
fi

if [ ! $hbase_path ]; then
    declare -x hbase_path
    discover_hbase_path
fi

# check if 'ant' can be found
if [ ! `which ant 2>/dev/null`]; then
    if [ ! -e apache-ant-1.9.2 ]; then
        wget http://mirror.nus.edu.sg/apache/ant/binaries/apache-ant-1.9.2-bin.tar.gz
        tar zxf apache-ant-1.9.2-bin.tar.gz
    fi
    export PATH=apache-ant-1.9.2/bin:$PATH
fi

echo "Using $hadoop_path as hadoop home"
echo "Using $hbase_path as hbase home"
echo 
echo "Copying libs into local build directory"
mkdir -p ./lib/

if ls ${hbase_path}/lib/commons-logging*.jar &> /dev/null; then
    cp ${hbase_path}/lib/commons-logging*.jar ./lib/
else
    echo "Cannot find commons-logging jars in hbase home"
    exit 1
fi

if ls ${hbase_path}/hbase-*.jar &> /dev/null; then
    cp ${hbase_path}/hbase-*.jar ./lib/
else
    echo "Cannot find hbase jars in hbase home"
    exit 1
fi

if ls ${hbase_path}/lib/commons-codec*.jar &> /dev/null; then
    cp ${hbase_path}/lib/commons-codec*.jar ./lib/
else
    candidates=(`find / -name "*commons-codec*.jar"`)
    if [ -z  $candidates ]; then
        echo "Cannot find commons-codec jar"
        exit 1
    else
        cp ${candidates[0]} ./lib/
    fi
fi

# Special case for Cloudera Hadoop Distribution
candidates=(`find /usr/lib/hadoop -maxdepth 1 -type f -name "*hadoop-common*.jar" | grep -v test`)
if [ $candidates ]; then
    cp ${candidates[0]} ./lib/
fi

# Distributions of Hadoop use different names for hadoop core.
# To Do: Explicitly list hadoop-core name patterns for each Hadoop distro
if ls ${hadoop_path}/hadoop-*-core.jar; then
    cp ${hadoop_path}/hadoop-*-core.jar ./lib/
elif ls ${hadoop_path}/hadoop-core-*.jar; then
    cp ${hadoop_path}/hadoop-core-*.jar ./lib/
else
    echo "Cannot find hadoop-core jar file in hadoop home"
    exit 1
fi


if ls ${hadoop_path}/contrib/streaming/hadoop-streaming-*.jar &> /dev/null; then 
    cp ${hadoop_path}/contrib/streaming/hadoop-streaming-*.jar ./lib/
else
    echo "Cannot find hadoop-streaming jar in hadoop home"i
    exit 1
fi

# If JAVA_HOME is not defined, try to use Oracle's jdk first. If Oracle Java is 
# not installed either, try to use openjdk.
if [ -z "$JAVA_HOME" ]; then
  for candidate in \
    /usr/lib/jvm/java-6-sun \
    /usr/lib/jvm/java-1.6.0-sun-1.6.0.*/jre/ \
    /usr/lib/jvm/java-1.6.0-sun-1.6.0.* \
    /usr/lib/jvm/j2sdk1.6-oracle \
    /usr/lib/jvm/j2sdk1.6-oracle/jre \
    /usr/lib/j2sdk1.6-sun \
    /usr/java/jdk1.6* \
    /usr/java/jre1.6* \
    /Library/Java/Home \
    /usr/java/default \
    /usr/lib/jvm/default-java \
    /usr/lib/jvm/java-openjdk \
    /usr/lib/jvm/jre-openjdk \
    /usr/lib/jvm/java-1.6.0-openjdk-1.6.* \
    /usr/lib/jvm/jre-1.6.0-openjdk* ; do
    if [ -e $candidate/bin/java ]; then
      export JAVA_HOME=$candidate
      break
    fi
  done
fi

echo "Building hadoopy_hbase.jar"
ant
