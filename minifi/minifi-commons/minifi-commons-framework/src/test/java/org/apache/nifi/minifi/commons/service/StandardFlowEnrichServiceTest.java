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

package org.apache.nifi.minifi.commons.service;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.randomUUID;
import static org.apache.nifi.minifi.commons.service.StandardFlowEnrichService.PARENT_SSL_CONTEXT_SERVICE_NAME;
import static org.apache.nifi.minifi.commons.service.StandardFlowEnrichService.DEFAULT_SSL_CONTEXT_SERVICE_NAME;
import static org.apache.nifi.minifi.commons.service.StandardFlowEnrichService.SITE_TO_SITE_PROVENANCE_REPORTING_TASK_NAME;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationIntrospector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.minifi.commons.api.MiNiFiProperties;
import org.apache.nifi.properties.StandardReadableProperties;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class StandardFlowEnrichServiceTest {

    private static final Path DEFAULT_FLOW_JSON = Path.of("src/test/resources/default_flow.json");

    @Test
    public void testFlowIsLeftIntactIfEnrichingIsNotNecessary() {
        Map<String, String> properties = Map.of();
        VersionedDataflow testFlow = loadDefaultFlow();

        FlowEnrichService testFlowEnrichService = new StandardFlowEnrichService(new StandardReadableProperties(properties));
        testFlowEnrichService.enrichFlow(testFlow);

        byte[] testFlowBytes = flowToString(testFlow).getBytes(UTF_8);
        byte[] enrichedFlowBytes = flowToString(testFlow).getBytes(UTF_8);
        assertArrayEquals(testFlowBytes, enrichedFlowBytes);
    }

    @Test
    public void testMissingRootGroupIdsAreFilledIn() {
        Map<String, String> properties = Map.of();
        VersionedDataflow testFlow = loadDefaultFlow();
        testFlow.getRootGroup().setIdentifier(null);
        testFlow.getRootGroup().setInstanceIdentifier(null);
        UUID expectedIdentifier = randomUUID();

        try (MockedStatic<UUID> uuid = mockStatic(UUID.class)) {
            uuid.when(UUID::randomUUID).thenReturn(expectedIdentifier);

            FlowEnrichService testFlowEnrichService = new StandardFlowEnrichService(new StandardReadableProperties(properties));
            testFlowEnrichService.enrichFlow(testFlow);

            assertEquals(expectedIdentifier.toString(), testFlow.getRootGroup().getIdentifier());
            assertEquals(expectedIdentifier.toString(), testFlow.getRootGroup().getInstanceIdentifier());
        }
    }

    @Test
    public void testCommonSslControllerServiceIsAddedWithBundleVersionAndProcessorControllerServiceIsOverridden() {
        Map<String, String> properties = securityProperties(true);
        VersionedDataflow testFlow = loadDefaultFlow();
        Bundle bundle = bundle("org.apache.nifi", "nifi-ssl-context-service-nar", StringUtils.EMPTY);
        String originalSslControllerServiceId = "original_ssl_controller_service_id";
        testFlow.getRootGroup()
            .setProcessors(Set.of(
                processor(bundle, "processor_1", originalSslControllerServiceId),
                processor(bundle, "processor_2", originalSslControllerServiceId)
            ));

        FlowEnrichService testFlowEnrichService = new StandardFlowEnrichService(new StandardReadableProperties(properties));
        testFlowEnrichService.enrichFlow(testFlow);

        assertEquals(1, testFlow.getRootGroup().getControllerServices().size());
        VersionedControllerService sslControllerService = testFlow.getRootGroup().getControllerServices().iterator().next();
        assertEquals(PARENT_SSL_CONTEXT_SERVICE_NAME, sslControllerService.getName());
        assertEquals(StringUtils.EMPTY, sslControllerService.getBundle().getVersion());
        Set<VersionedProcessor> processors = testFlow.getRootGroup().getProcessors();
        assertEquals(2, processors.size());
        assertTrue(
            processors.stream()
                .map(VersionedConfigurableExtension::getProperties)
                .map(props -> props.get(DEFAULT_SSL_CONTEXT_SERVICE_NAME))
                .allMatch(controllerServiceName -> sslControllerService.getInstanceIdentifier().equals(controllerServiceName))
        );
    }

    @Test
    public void testProvenanceReportingTaskIsAdded() {
        Map<String, String> properties = Map.of(
            MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_COMMENT.getKey(), "comment",
            MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_SCHEDULING_STRATEGY.getKey(), "timer_driven",
            MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_SCHEDULING_PERIOD.getKey(), "10 sec",
            MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_DESTINATION_URL.getKey(), "http://host:port/destination",
            MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_INPUT_PORT_NAME.getKey(), "input_port",
            MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_INSTANCE_URL.getKey(), "http://host:port/input",
            MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_COMPRESS_EVENTS.getKey(), "true",
            MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_BATCH_SIZE.getKey(), "1000",
            MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_COMMUNICATIONS_TIMEOUT.getKey(), "30 sec"
        );
        VersionedDataflow testFlow = loadDefaultFlow();

        FlowEnrichService testFlowEnrichService = new StandardFlowEnrichService(new StandardReadableProperties(properties));
        testFlowEnrichService.enrichFlow(testFlow);

        List<VersionedReportingTask> reportingTasks = testFlow.getReportingTasks();
        assertEquals(1, reportingTasks.size());
        VersionedReportingTask provenanceReportingTask = reportingTasks.get(0);
        assertEquals(SITE_TO_SITE_PROVENANCE_REPORTING_TASK_NAME, provenanceReportingTask.getName());
        assertEquals(properties.get(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_COMMENT.getKey()), provenanceReportingTask.getComments());
        assertEquals(properties.get(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_SCHEDULING_STRATEGY.getKey()), provenanceReportingTask.getSchedulingStrategy());
        assertEquals(properties.get(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_SCHEDULING_PERIOD.getKey()), provenanceReportingTask.getSchedulingPeriod());
        Map<String, String> provenanceReportingTaskProperties = provenanceReportingTask.getProperties();
        assertEquals(properties.get(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_INPUT_PORT_NAME.getKey()), provenanceReportingTaskProperties.get("Input Port Name"));
        assertEquals(properties.get(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_DESTINATION_URL.getKey()), provenanceReportingTaskProperties.get("Destination URL"));
        assertEquals(properties.get(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_COMPRESS_EVENTS.getKey()), provenanceReportingTaskProperties.get("Compress Events"));
        assertEquals(properties.get(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_BATCH_SIZE.getKey()), provenanceReportingTaskProperties.get("Batch Size"));
        assertEquals(properties.get(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_COMMUNICATIONS_TIMEOUT.getKey()),
            provenanceReportingTaskProperties.get("Communications Timeout"));
        assertEquals(properties.get(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_INSTANCE_URL.getKey()), provenanceReportingTaskProperties.get("Instance URL"));
    }

    private VersionedDataflow loadDefaultFlow() {
        try {
            String flowString = Files.readString(DEFAULT_FLOW_JSON);
            return flowFromString(flowString);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private VersionedDataflow flowFromString(String flow) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setAnnotationIntrospector(new JakartaXmlBindAnnotationIntrospector(objectMapper.getTypeFactory()));
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try {
            return objectMapper.readValue(flow, VersionedDataflow.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String flowToString(VersionedDataflow versionedDataflow) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setAnnotationIntrospector(new JakartaXmlBindAnnotationIntrospector(objectMapper.getTypeFactory()));
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try {
            return objectMapper.writeValueAsString(versionedDataflow);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> securityProperties(Boolean useParentSslControllerService) {
        return Map.of(
            MiNiFiProperties.NIFI_MINIFI_SECURITY_KEYSTORE.getKey(), "path/to/keystore.jks",
            MiNiFiProperties.NIFI_MINIFI_SECURITY_KEYSTORE_TYPE.getKey(), "jks",
            MiNiFiProperties.NIFI_MINIFI_SECURITY_KEYSTORE_PASSWD.getKey(), "password123",
            MiNiFiProperties.NIFI_MINIFI_SECURITY_KEY_PASSWD.getKey(), "password456",
            MiNiFiProperties.NIFI_MINIFI_SECURITY_TRUSTSTORE.getKey(), "path/to/truststore.jks",
            MiNiFiProperties.NIFI_MINIFI_SECURITY_TRUSTSTORE_TYPE.getKey(), "jks",
            MiNiFiProperties.NIFI_MINIFI_SECURITY_TRUSTSTORE_PASSWD.getKey(), "password789",
            MiNiFiProperties.NIFI_MINIFI_SECURITY_SSL_PROTOCOL.getKey(), "TLS1.2",
            MiNiFiProperties.NIFI_MINIFI_FLOW_USE_PARENT_SSL.getKey(), useParentSslControllerService.toString()
        );
    }

    private Bundle bundle(String group, String artifact, String version) {
        Bundle bundle = new Bundle();
        bundle.setGroup(group);
        bundle.setArtifact(artifact);
        bundle.setVersion(version);
        return bundle;
    }

    private VersionedProcessor processor(Bundle bundle, String name, String originalSslControllerServiceId) {
        VersionedProcessor versionedProcessor = new VersionedProcessor();
        versionedProcessor.setIdentifier(randomUUID().toString());
        versionedProcessor.setName(name);
        versionedProcessor.setBundle(bundle);
        versionedProcessor.setProperties(new HashMap<>(Map.of(DEFAULT_SSL_CONTEXT_SERVICE_NAME, originalSslControllerServiceId)));
        return versionedProcessor;
    }
}
