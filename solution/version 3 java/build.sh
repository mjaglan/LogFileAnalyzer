if [ ! -d classes ]; then
        mkdir classes;
fi

# Compile LogFileAnalyzer
javac -classpath $HADOOP_HOME/hadoop-core-1.1.2.jar:$HADOOP_HOME/lib/commons-cli-1.2.jar -d ./classes LogFileAnalyzer.java

# Create the Jar
jar -cvf LogFileAnalyzer.jar -C ./classes/ .
 
# Copy the jar file to the Hadoop distributions
cp LogFileAnalyzer.jar $HADOOP_HOME/bin/ 

