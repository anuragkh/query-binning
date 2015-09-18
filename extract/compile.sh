mkdir -p extract_classes
rm -rf extract_classes/*
javac -classpath ~/hadoop-1.2.1/hadoop-core-1.2.1.jar -d extract_classes Extract.java
jar -cvf extract.jar -C extract_classes/ .
