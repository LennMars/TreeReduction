current_local_raw=`pwd`
current_local="file://${current_local_raw}"
current_hdfs="some_place_in_hdfs"
jarname=treereduction_2.9.1-0.1.jar

usage="Usage: $CMDNAME [-d | -l (dist|local)] [-a 1|2 (algorithm 1|2)] [-c (compress)] [-m <memo>] [-r <name of reduce class>] <input>"

if [ $# -eq 0 ]; then
  echo $usage 1>&2
  exit 0
fi

#default values
mode=1
value="MaxPathSumReduce"
compress=""
is_hdfs="FALSE"

while getopts dla:m:r:c OPT
do
  case $OPT in
    "d" ) is_hdfs="TRUE" ;;
    "l" ) is_hdfs="FALSE" ;;
    "a" ) mode="${OPTARG}" ;;
    "c" ) compress="-c" ;;
    "m" ) memo="$OPTARG" ;;
    "r" ) value="$OPTARG" ;;
      * ) echo $usage 1>&2
          exit 1 ;;
  esac
done

shift `expr $OPTIND - 1` #discard options from args
testcase=`echo $1 | sed -e "s/^.*\///"`
echo "testcase = ${testcase}"

logdir="experiment/${testcase}_`date +%y%m%d_%T`"
mkdir -p ${logdir}
cp "exec.sh" ${logdir}/
echo "memo: ${memo}" >> ${logdir}/std.log
echo "value: ${value}" >> ${logdir}/std.log

#refresh jar
if [ -e "target/scala-2.9.1/${jarname}" ]
then
 rm -f ./${jarname}
 cp "target/scala-2.9.1/${jarname}" ./
fi

if [ $is_hdfs = "TRUE" ]
then
    echo "distributed mode"
    echo "environment: distributed" >> ${logdir}/std.log
    hadoop dfsadmin -report >> ${logdir}/std.log
    input="${current_hdfs}/${1}"
    output="${current_hdfs}/output/${testcase}"
    hadoop fs -test -d output
    if [ $? -ne 0 ]; then hadoop fs -mkdir output; fi
    hadoop fs -rmr output/${testcase}*
    time hadoop jar ${jarname} -mode ${mode} -reduce ${value} ${compress} ${input} ${output} ${current_local}/${logdir} 3>&1 1>&2 2>&3 1>>"${logdir}/std.log" | tee "${logdir}/err.log"
else
    echo "local mode"
    echo "environment: local" >> ${logdir}/std.log
    input=$1
    output="${current_local}/output/${testcase}"
    if [ ! -e "./output" ]; then mkdir output; fi
    rm -rf output/${testcase}*
    time hadoop --config conf_local/ jar ${jarname} -mode ${mode} -reduce ${value} ${compress} ${input} ${output} ${current}/${logdir} 3>&1 1>&2 2>&3 1>>"${logdir}/std.log" | tee "${logdir}/err.log"
fi

