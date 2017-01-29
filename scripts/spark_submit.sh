#!/usr/bin/env bash
set -e

. $(dirname $0)/env.sh

CLASS=$1
DATE=$(date +"%Y%m%d%H%M")
LOGFILE=log_${DATE}.log
BUSLOGFILE=businessLog_${DATE}.log
shift;

$(dirname $0)/build_assembly.sh

ENV=test
APP_CONFIG=${APP_DIR}/conf/${ENV}.conf

touch ${LOG_DIR}/${LOGFILE}
ln -sf ${LOG_DIR}/${LOGFILE} ${LOG_DIR}/log_latest.log

touch ${LOG_DIR}/${BUSLOGFILE}
ln -sf ${LOG_DIR}/${BUSLOGFILE} ${LOG_DIR}/businessLog_latest.log

# add the dependencies to the class path using "--driver-class-path" otherwise the BLAS libraries cannot be found by spark
nohup ${SPARK_HOME}/bin/spark-submit --jars ${APP_DIR}/languagedetector-assembly-1.0-deps.jar --class biz.meetmatch.$CLASS --driver-memory 4g --driver-java-options "-Dconfig.file=${APP_CONFIG} -DbusinessLogFileName=/tmp/${BUSLOGFILE}" ${APP_DIR}/languagedetector_2.11-1.0.jar "$@" > ${LOG_DIR}/${LOGFILE} 2>&1 &

tail -f ${LOG_DIR}/${LOGFILE}