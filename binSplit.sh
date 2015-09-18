FileName=$1
Reducers=100
MaxCount=10000
Delim=124

hadoop=hadoop
JARS_HOME=./jars

$hadoop jar $JARS_HOME/extract.jar \
	binning.Extract \
	s3://succinct-datasets/$FileName \
	/bin-temp \
	$Delim \
	$MaxCount \
	$Reducers

$hadoop jar $JARS_HOME/segregate.jar \
	binning.Segregate \
	/bin-temp \
	/$FileName-bins \
	$Delim \
	$Reducers

$hadoop fs -rmr /bin-temp
