#!/usr/bin/env bash

set -e

cd "$(dirname "$0")/../.." || exit

usage() {
  echo "Usage: $(basename "$0") [-h|--help]
where :
  -h| --help Display this help text
" 1>&2
  exit 1
}

if [[ ($# -ne 0) || ( $1 == "--help") ||  $1 == "-h" ]]; then
  usage
fi

readTagFromRelease() {
  grep "^scm.tag=" release.properties | head -n1 | sed "s/scm.tag=//"
}

JAVA_VERSION="$(mvn help:evaluate -Dexpression=jdk.version -q -DforceStdout)"
if ! [[ $(java -version 2>&1 | head -n 1 | cut -d'"' -f2) =~ ^$JAVA_VERSION.* ]]; then
  echo "Error: invalid Java version - Java $JAVA_VERSION required"
  exit 1
fi

if ! [[ $(which gpg) ]]; then
  echo "Error: gpg executable not found"
  exit 1
fi

# get current branch we're releasing off
BRANCH="$(git branch --show-current)"

# use the maven release plugin to make the pom changes and tag
mvn release:prepare \
  -DautoVersionSubmodules=true \
  -Darguments="-DskipTests -Dmaven.javadoc.skip=true -Ppython" \
  -Ppython

TAG="$(readTagFromRelease)"

# clean up leftover release artifacts
mvn release:clean

# deploy to maven central
git checkout "$TAG"

mvn clean deploy -Pcentral,python -DskipTests | tee build_2.12.log

./build/change-scala-version.sh 2.13
mvn clean deploy -Pcentral,python -DskipTests | tee build_2.13.log

# reset pom changes
./build/change-scala-version.sh 2.12

# go back to original branch
git checkout "$BRANCH"
