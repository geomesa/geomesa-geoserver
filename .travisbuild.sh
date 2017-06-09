#!/bin/bash
# Abort on Error
set -e

export PING_SLEEP=60s
export WORKDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export BUILD_OUTPUT=$WORKDIR/build.out

touch $BUILD_OUTPUT

# set up a repeating loop to send some output to Travis
echo [INFO] $(date -u '+%F %T UTC') - build starting
bash -c "while true; do sleep $PING_SLEEP; echo [INFO] \$(date -u '+%F %T UTC') - build continuing...; done" &
PING_LOOP_PID=$!

# build using the maven executable, not the zinc maven compiler (which uses too much memory)
mvn clean test 
RESULT=${PIPESTATUS[0]} # capture the status of the maven build

# dump out the end of the build log, to show success or errors
tail -500 $BUILD_OUTPUT

if [[ $RESULT -ne 0 ]]; then
  echo -e "[ERROR] Build failed!\n"
fi

# nicely terminate the ping output loop
kill $PING_LOOP_PID

# exit with the result of the maven build to pass/fail the travis build
exit $RESULT
