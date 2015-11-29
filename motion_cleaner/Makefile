all: cl.jar
clean:
	rm -rf *.class *.jar

cl.jar: DuplicateCleaning.class
	jar cf cl.jar *.class

DuplicateCleaning.class: DuplicateCleaning.java
	javac -classpath /usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.1.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.1.jar *.java
run: cl.jar
	hadoop fs -rm -r -f input/motion_clean
	hadoop jar cl.jar DuplicateCleaning
