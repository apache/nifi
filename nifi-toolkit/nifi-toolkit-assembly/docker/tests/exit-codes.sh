#!/bin/bash

set -xuo pipefail

VERSION=${1:-}

image_name=apache/nifi-toolkit:${VERSION}

echo "Testing return values on missing input:"
docker run --rm "${image_name}"
test 0 -eq $? || exit 1

echo "Testing return values on invalid input for all commands:"
docker run --rm "${image_name}" encrypt-config invalid 1>/dev/null 2>&1
test 2 -eq $? || exit 1

docker run --rm "${image_name}" cli invalid 1>/dev/null 2>&1
test 255 -eq $? || exit 1
