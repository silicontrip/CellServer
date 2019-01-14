#!/bin/sh

mainclass=$( basename $0 | sed s/.sh$//)

#echo $mainclass

java -cp cellserver.jar:lib/json-java.jar:lib/mongo-java-driver-3.0.2.jar:lib/s2-geometry-java.jar:lib/guava-18.0.jar $mainclass "$@"
