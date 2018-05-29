#!/bin/bash

set -exuo pipefail

VERSION=${1:-}

trap "{ docker rm -f nifi-${VERSION}-integration-test; }" EXIT

echo "Checking that all files are owned by NiFi"
test -z $(docker run --rm --entrypoint /bin/bash apache/nifi:${VERSION} -c "find /opt/nifi ! -user nifi")

echo "Checking environment variables"
test "/opt/nifi/nifi-${VERSION}" = "$(docker run --rm --entrypoint /bin/bash apache/nifi:${VERSION} -c 'echo -n $NIFI_HOME')"
test "/opt/nifi/nifi-${VERSION}/logs" = "$(docker run --rm --entrypoint /bin/bash apache/nifi:${VERSION} -c 'echo -n $NIFI_LOG_DIR')"
test "/opt/nifi/nifi-${VERSION}/run" = "$(docker run --rm --entrypoint /bin/bash apache/nifi:${VERSION} -c 'echo -n $NIFI_PID_DIR')"
test "/opt/nifi" = "$(docker run --rm --entrypoint /bin/bash apache/nifi:${VERSION} -c 'echo -n $NIFI_BASE_DIR')"

echo "Starting NiFi container..."
docker run -d --name nifi-${VERSION}-integration-test apache/nifi:${VERSION}

for i in $(seq 1 10) :; do
    if docker exec nifi-${VERSION}-integration-test bash -c "ss -ntl | grep 8080"; then
        break
    fi
    sleep 10
done

echo "Checking system diagnostics"
test ${VERSION} = $(docker exec nifi-${VERSION}-integration-test bash -c "curl -s localhost:8080/nifi-api/system-diagnostics | jq .systemDiagnostics.aggregateSnapshot.versionInfo.niFiVersion -r")

echo "Stopping NiFi container"
time docker stop nifi-${VERSION}-integration-test