function check_hive(){
    CHECK_GAP=600
    DB=$1
		PARTITION=$2
    echo "check hive file: $DB"
    hadoop_test_flag=1
    count=0;
    while [ $hadoop_test_flag -ne 0 ]
    do
		s=`$HIVE -e "show partitions $DB"|grep "$PARTITION" | wc -l`
		echo $s
		if [ $s -ge 1 ]
		then
			hadoop_test_flag=0
		fi  

        if [ $hadoop_test_flag -ne 0 ]
        then
            ((count=$count+1));
            echo "$count check ...";
            sleep $CHECK_GAP;
        fi
    done
    echo "check hive : $DB success"
}

function check_hdfs(){
    CHECK_GAP=60
    HADOOP_PATH=$1
		SIZE=$2

    hadoop=$HADOOP
    echo "check hadoop file: $HADOOP_PATH"
    hadoop_test_flag=1
    count=0;
    while [ $hadoop_test_flag -ne 0 ]
    do
		h=`$HADOOP fs -ls $HADOOP_PATH | grep -o -e "20..-..-.. ..:.."| sort |tail -1`
		nnn=`$HADOOP fs -dus -h $HADOOP_PATH | awk -F'G' '{print int($1)}'` 
		echo $h
		echo $SIZE $nnn
		d=`date -d"$h 20 min" +%s`
		nd=`date +%s`
		if [ $d -lt $nd -a $nnn -ge $SIZE ] 
		then
			hadoop_test_flag=0
		fi  

        if [ $hadoop_test_flag -ne 0 ]
        then
            ((count=$count+1));
            echo "$count check ...";
            sleep $CHECK_GAP;
        fi
    done
    echo "check hadoop file: $HADOOP_PATH success"
}

date1=`date -d"$date" +"%Y/%m/%d"`
FILE="filename"
DB="tablename"
check_hdfs  $FILE 120
check_hive $DB "dt=$date"
