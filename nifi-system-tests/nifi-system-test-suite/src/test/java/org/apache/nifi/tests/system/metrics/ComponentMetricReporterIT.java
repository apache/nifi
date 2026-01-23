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
package org.apache.nifi.tests.system.metrics;

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

public class ComponentMetricReporterIT extends NiFiSystemIT {

    private static final String REPORTER_IMPLEMENTATION = "nifi.component.metric.reporter.implementation";

    private static final String REPORTER_CLASS = "org.apache.nifi.controller.metrics.SystemTestComponentMetricReporter";

    private static final String GAUGE_RECORD_LOG = "%s.GaugeRecord".formatted(REPORTER_CLASS);

    private static final String COUNTER_RECORD_LOG = "%s.CounterRecord".formatted(REPORTER_CLASS);

    @Override
    protected Map<String, String> getNifiPropertiesOverrides() {
        return Map.of(REPORTER_IMPLEMENTATION, REPORTER_CLASS);
    }

    @Test
    void testUpdateMetricReported() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity updateMetric = getClientUtil().createProcessor("UpdateMetric");
        getNifiClient().getProcessorClient().runProcessorOnce(updateMetric);
        getClientUtil().waitForStoppedProcessor(updateMetric.getId());

        final String componentId = updateMetric.getId();
        assertLogComponentIdFound(GAUGE_RECORD_LOG, componentId);
        assertLogComponentIdFound(COUNTER_RECORD_LOG, componentId);
    }

    private Optional<Path> findReportedLog(final String fileNameSearch) throws IOException {
        final Path instanceDirectory = getNiFiInstance().getInstanceDirectory().toPath();
        try (Stream<Path> userDirectoryPaths = Files.list(instanceDirectory)) {
            return userDirectoryPaths.filter(path -> path.getFileName().toString().startsWith(fileNameSearch)).findFirst();
        }
    }

    private void assertLogComponentIdFound(final String fileNameSearch, final String componentId) throws IOException {
        final Optional<Path> reportedLogFound = findReportedLog(fileNameSearch);
        assertTrue(reportedLogFound.isPresent(), "Component Metric Reporter [%s] log not found".formatted(fileNameSearch));

        final Path reportedLog = reportedLogFound.get();
        final String log = Files.readString(reportedLog);

        assertTrue(log.contains(componentId), "Update Metric ID [%s] not found in log [%s]".formatted(componentId, log));
    }
}
