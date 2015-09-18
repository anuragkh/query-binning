mkdir -p segregate_classes
rm -rf segregate_classes/*
javac -classpath ~/hadoop-1.2.1/hadoop-core-1.2.1.jar -d segregate_classes Segregate.java
jar -cvf segregate.jar -C segregate_classes/ .
