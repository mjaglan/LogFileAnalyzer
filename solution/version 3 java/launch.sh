#!/bin/bash

if [ $# -ne 1 ]; then
    echo 1>&2 Usage: [Count of top users?]
    echo "e.g. ./launch.sh 5"
    k=$((RANDOM%10+1))
    echo "Let's take some random k="$k
    # exit -1
else
	k=$1
	echo "You've choosen k="$1
fi

cd /root/software/hadoop-1.1.2/
./MultiNodesOneClickStartUp.sh /root/software/jdk1.6.0_33/ nodes
sleep 10

# hadoop dfs -ls
# hadoop dfs -rmr output
# hadoop dfs -rmr input
hadoop dfs -mkdir input
hadoop dfs -put /root/MoocHomeworks/LogFileAnalyzer/input/log_file_input.txt input/

cd /root/MoocHomeworks/LogFileAnalyzer
./build.sh
hadoop jar /root/MoocHomeworks/LogFileAnalyzer/LogFileAnalyzer.jar LogFileAnalyzer input output $k

hadoop dfs -ls
hadoop dfs -cat output/part-r-00000
outputFileName="/root/MoocHomeworks/LogFileAnalyzer/output/output.top.users.txt"
rm -rf $outputFileName
hadoop dfs -get output/part-r-00000  $outputFileName
cd /root/MoocHomeworks/LogFileAnalyzer

