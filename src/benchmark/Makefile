

all: run

%.class:%.java snaptree.jar
	javac -classpath .:snaptree.jar $<

run: ConcurrentSearchBenchmark.class
	java -classpath .:snaptree.jar ConcurrentSearchBenchmark \
		4 4 0.02 2 10000 1000 0.5 0.5 0.0 1

clean:
	rm -rf *.class a.out core

