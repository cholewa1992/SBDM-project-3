all: cl.jar
clean:
	rm -rf *.class *.jar

cl.jar: RegexCleaning.class
	jar cf cl.jar *.class

RegexCleaning.class: RegexCleaning.java
	javac -classpath /usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.1.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.1.jar *.java
run: cl.jar
	hadoop fs -rm -r -f input/switch_clean
	hadoop jar cl.jar RegexCleaning
