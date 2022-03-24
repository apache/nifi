#!/bin/bash

set -exuo pipefail

VERSION=$1
IMAGE=apache/nifi-toolkit:${VERSION}
CONTAINER=nifi-toolkit-$VERSION-tls-toolkit-integration-test

TOKEN=D40F6B95-801F-4800-A1E1-A9FCC712E0BD

trap " { docker rm -f $CONTAINER ; } " EXIT

echo "Starting CA server using the tls-toolkit server command"
docker run -d --name $CONTAINER $IMAGE tls-toolkit server -t $TOKEN -c $CONTAINER

echo "Requesting client certificate using the tls-toolkit client command"
docker run --rm --link $CONTAINER $IMAGE tls-toolkit client -t $TOKEN -c $CONTAINER
