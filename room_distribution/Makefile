all: dc.jar
clean:
	rm -rf *.class *.jar

dc.jar: DistributionCounter.class
	jar cf dc.jar *.class

DistributionCounter.class: DistributionCounter.java
	javac -classpath /usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.1.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.1.jar *.java
run: dc.jar
	hadoop fs -rm -r -f output/
	hadoop jar dc.jar DistributionCounter input/switch_clean output/
	hadoop fs -cat output/part-r-00000 > swtich_output.txt
	hadoop fs -rm -r -f output/
	hadoop jar dc.jar DistributionCounter input/motion_clean output/
	hadoop fs -cat output/part-r-00000 > motion_output.txt
