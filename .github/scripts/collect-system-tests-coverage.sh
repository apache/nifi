#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Collects JaCoCo coverage files from system tests, merges them, and generates an XML report.
# This script is used by the code-coverage GitHub Actions workflow.

set -e

# Collect all jacoco.exec files from spawned NiFi instance directories
EXEC_FILES=""
for dir in nifi-system-tests/nifi-system-test-suite/target/*/coverage; do
  if [ -f "$dir/jacoco.exec" ]; then
    EXEC_FILES="$EXEC_FILES $dir/jacoco.exec"
    echo "Found: $dir/jacoco.exec"
  fi
done

# Also collect jacoco-it.exec from stateless tests (runs in-process, captured by JaCoCo agent)
if [ -f "nifi-system-tests/nifi-stateless-system-test-suite/target/jacoco-it.exec" ]; then
  EXEC_FILES="$EXEC_FILES nifi-system-tests/nifi-stateless-system-test-suite/target/jacoco-it.exec"
  echo "Found: nifi-system-tests/nifi-stateless-system-test-suite/target/jacoco-it.exec"
fi

if [ -z "$EXEC_FILES" ]; then
  echo "No coverage files found"
  exit 0
fi

# Download JaCoCo CLI
JACOCO_VERSION=$(./mvnw help:evaluate -Dexpression=jacoco.version -q -DforceStdout 2>/dev/null)
./mvnw dependency:copy -Dartifact=org.jacoco:org.jacoco.cli:${JACOCO_VERSION}:jar:nodeps -DoutputDirectory=target/jacoco-cli

# Merge all exec files into one
mkdir -p target/system-tests-coverage
echo "Merging coverage files..."
java -jar target/jacoco-cli/org.jacoco.cli-${JACOCO_VERSION}-nodeps.jar merge \
  $EXEC_FILES \
  --destfile target/system-tests-coverage/jacoco-merged.exec

# Build classfiles by merging all module classes into a single directory
# This avoids duplicate class errors that occur when same class exists in multiple modules
mkdir -p target/merged-classes

# Extract module names from nifi-code-coverage/pom.xml (nifi-*, minifi-*, c2-* only)
# Exclude nifi-mock as it has class files that JaCoCo cannot analyze
MODULES=$(grep '<artifactId>' nifi-code-coverage/pom.xml | sed 's/.*<artifactId>//' | sed 's/<.*//' | grep -E '^(nifi-|minifi-|c2-)' | grep -v 'nifi-code-coverage' | grep -v 'nifi-mock' | sort -u)

SOURCEFILES_ARGS=""
MODULE_COUNT=0

for module in $MODULES; do
  classdir=$(find . -type d -path "*/${module}/target/classes" 2>/dev/null | head -1)
  if [ -n "$classdir" ] && [ -d "$classdir" ]; then
    # Copy classes to merged directory (later copies overwrite, deduplicating)
    cp -r "$classdir"/* target/merged-classes/ 2>/dev/null || true
    MODULE_COUNT=$((MODULE_COUNT + 1))
    # Also add corresponding source directory
    srcdir=$(echo "$classdir" | sed 's|/target/classes|/src/main/java|')
    if [ -d "$srcdir" ]; then
      SOURCEFILES_ARGS="$SOURCEFILES_ARGS --sourcefiles $srcdir"
    fi
  fi
done

echo "Merged classes from $MODULE_COUNT modules"

echo "Generating XML report from merged classes..."
java -jar target/jacoco-cli/org.jacoco.cli-${JACOCO_VERSION}-nodeps.jar report \
  target/system-tests-coverage/jacoco-merged.exec \
  --classfiles target/merged-classes \
  $SOURCEFILES_ARGS \
  --xml target/system-tests-coverage/jacoco.xml 2>&1 || true

# Check if report was generated
if [ -f "target/system-tests-coverage/jacoco.xml" ]; then
  echo "=== Generated XML report ==="
  ls -la target/system-tests-coverage/jacoco.xml
  head -20 target/system-tests-coverage/jacoco.xml
else
  echo "WARNING: No XML report was generated"
fi

