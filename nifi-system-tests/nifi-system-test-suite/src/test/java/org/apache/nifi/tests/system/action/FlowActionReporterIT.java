/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.tests.system.action;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlowActionReporterIT extends NiFiSystemIT {

    private static final String FLOW_ACTION_REPORTER_IMPLEMENTATION = "nifi.flow.action.reporter.implementation";

    private static final String FLOW_ACTION_REPORTER_CLASS = "org.apache.nifi.action.SystemTestFlowActionReporter";

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        return Map.of(FLOW_ACTION_REPORTER_IMPLEMENTATION, FLOW_ACTION_REPORTER_CLASS);
    }

    @Test
    void testCreateProcessorReported() throws NiFiClientException, IOException {
        final ProcessorEntity generateFlowFile = getClientUtil().createProcessor("GenerateFlowFile");

        final Optional<Path> reportedLogFound = findReportedLog();
        assertTrue(reportedLogFound.isPresent(), "Flow Action Reporter log not found");

        final Path reportedLog = reportedLogFound.get();
        final String log = Files.readString(reportedLog);

        final String componentId = generateFlowFile.getId();
        assertTrue(log.contains(componentId), "GenerateFlowFile ID [%s] not found in log [%s]".formatted(componentId, log));
    }

    private Optional<Path> findReportedLog() throws IOException {
        final Path instanceDirectory = getNiFiInstance().getInstanceDirectory().toPath();
        try (Stream<Path> userDirectoryPaths = Files.list(instanceDirectory)) {
            return userDirectoryPaths.filter(path -> path.getFileName().toString().startsWith(FLOW_ACTION_REPORTER_CLASS)).findFirst();
        }
    }
}
