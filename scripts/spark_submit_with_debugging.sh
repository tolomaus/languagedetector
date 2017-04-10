#!/usr/bin/env bash
set -e

. $(dirname $0)/spark_submit_prep.sh "$@"
shift;
shift;

echo "Start an IntelliJ remote debugging session to port 5005"
# add the dependencies to the class path using "--driver-class-path" otherwise the guava lib v14.0 from spark is taken while languagedetect needs >= 18.0
${SPARK_HOME}/bin/spark-submit --jars ${APP_ENV_DIR}/${APP_NAME}-deps.jar --class biz.meetmatch.$CLASS --driver-memory 2g --driver-java-options "-Dconfig.file=${APP_CONFIG} -DbusinessLogFileName=${LOG_DIR}/${BUSLOGFILE} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" --driver-class-path "${APP_ENV_DIR}/${APP_NAME}-deps.jar" ${APP_ENV_DIR}/${APP_NAME}.jar "$@"
