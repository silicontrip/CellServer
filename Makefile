
CLASSES=Arguments.class CellServer.class UniformDistribution.class \
showcell.class mucelliter.class validatecell.class fielduse.class muquadcell.class

CP=lib/guava-18.0.jar:lib/mongo-java-driver-3.0.2.jar:lib/s2-geometry-java.jar:lib/json-java.jar:.

all: cellserver.jar

cellserver.jar: classes
	jar -cf cellserver.jar $(CLASSES) 

classes: $(CLASSES)

%.class: %.java
	javac  -classpath $(CP) -encoding utf8 -Xlint:deprecation -Xlint:unchecked  $<

clean:
	rm *.class

