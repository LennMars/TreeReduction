current_local_raw=`pwd`
current_local="file://${current_local_raw}"
current_hdfs="some_place_in_hdfs"
jarname=treereduction_2.9.1-0.1.jar

usage="Usage: $CMDNAME [OPTION]... [COMMAND]"

if [ $# -eq 0 ]; then
  echo $usage 1>&2
  exit 0
fi

#default values
compress=""
mode="local"
memo=""

while getopts dlm:c OPT
do
  case $OPT in
    "d" ) mode="dist" ;;
    "l" ) mode="local" ;;
    "c" ) compress="-c" ;;
    "m" ) memo="$OPTARG" ;;
      * ) echo $usage 1>&2
          exit 1 ;;
  esac
done

shift `expr $OPTIND - 1` #discard options from args

#refresh jar
if [ -e "target/scala-2.9.1/${jarname}" ]
then
 rm -f ./${jarname}
 cp "target/scala-2.9.1/${jarname}" ./
fi

function make_log() {
    logdir="experiment/${1}_`date +%y%m%d_%T`"
    mkdir -p ${logdir}
    cp "exec.sh" ${logdir}/
    touch "${logdir}/std.log"
    echo "memo: ${memo}" >> ${logdir}/std.log
    echo "compress: ${compress}" >> ${logdir}/std.log
    if [ $mode = "dist" ]
    then
        echo "environment: distributed" >> ${logdir}/std.log
        hadoop dfsadmin -report >> ${logdir}/std.log
    else
        echo "environment: local" >> ${logdir}/std.log
    fi
    echo $logdir
}

function make_output() {
    if [ $mode = "dist" ]
    then
        output="${current_hdfs}/output/$1"
        hadoop fs -test -d output
        if [ $? -ne 0 ]; then hadoop fs -mkdir output; fi
        hadoop fs -rmr output/${testcase}*
    else
        output="${current_local}/output/$1"
        if [ ! -e "./output" ]; then mkdir output; fi
        rm -rf output/${testcase}*
    fi
    echo $output
}

function make_input() {
    if [ $mode = "dist" ]
    then
        echo "${current_hdfs}/${1}"
    else
        echo $1
    fi
}

function make_conf() {
    if [ $mode = "dist" ]
    then
        echo ""
    else
        echo "--config conf_local/"
    fi
}

function tail() {
    echo $1 | sed -e "s/^.*\///"
}

function exec_unary() {
    conf=`make_conf`
    input=`make_input $3`
    testcase="$1_$2_`tail $3`"
    logdir=`make_log ${testcase}`
    output=`make_output ${testcase}`
    echo "command: $1 with $2" >> "${logdir}/std.log"
    echo "input: $input" >> "${logdir}/std.log"
    echo "logdir: $logdir" >> "${logdir}/std.log"
    echo "output: $output" >> "${logdir}/std.log"
    time hadoop $conf jar $jarname $compress "-$1" $2 $input $output 3>&1 1>&2 2>&3 1>>"${logdir}/std.log" | tee "${logdir}/err.log"
}

function exec_binary() {
    conf=`make_conf`
    input1=`make_input $3`
    input2=`make_input $4`
    testcase="$1_$2_`tail $3`_`tail $4`"
    logdir=`make_log ${testcase}`
    output=`make_output ${testcase}`
    echo "command: $1 with $2" >> "${logdir}/std.log"
    echo "input1: $input1" >> "${logdir}/std.log"
    echo "input2: $input2" >> "${logdir}/std.log"
    echo "logdir: $logdir" >> "${logdir}/std.log"
    echo "output: $output" >> "${logdir}/std.log"
    time hadoop $conf jar $jarname ${compress} "-$1" $2 $input1 $input2 $output 3>&1 1>&2 2>&3 1>>"${logdir}/std.log" | tee "${logdir}/err.log"
}

if [ $1 = "reduce1" ] || [ $1 = "reduce2" ]
then
    exec_unary $1 $2 $3
elif [ $1 = "zip" ]
then
    exec_binary $1 $2 $3 $4
else
    echo "unknown command"
fi


