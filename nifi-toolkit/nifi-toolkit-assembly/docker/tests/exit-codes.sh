#!/bin/bash

set -xuo pipefail

VERSION=${1:-}
IMAGE=apache/nifi-toolkit:${VERSION}

echo "Testing return values on missing input:"
docker run --rm $IMAGE
test 0 -eq $? || exit 1

echo "Testing return values on invalid input for all commands:"
docker run --rm $IMAGE encrypt-config invalid 1>/dev/null 2>&1
test 2 -eq $? || exit 1

docker run --rm $IMAGE s2s invalid 1>/dev/null 2>&1
test 0 -eq $? || exit 1

docker run --rm $IMAGE zk-migrator invalid 1>/dev/null 2>&1
test 0 -eq $? || exit 1

docker run --rm $IMAGE node-manager invalid 1>/dev/null 2>&1
test 1 -eq $? || exit 1

docker run --rm $IMAGE cli invalid 1>/dev/null 2>&1
test 255 -eq $? || exit 1

docker run --rm $IMAGE tls-toolkit invalid 1>/dev/null 2>&1
test 2 -eq $? || exit 1

docker run --rm $IMAGE file-manager invalid 1>/dev/null 2>&1
test 1 -eq $? || exit 1

docker run --rm $IMAGE flow-analyzer invalid 1>/dev/null 2>&1
test 1 -eq $? || exit 1
