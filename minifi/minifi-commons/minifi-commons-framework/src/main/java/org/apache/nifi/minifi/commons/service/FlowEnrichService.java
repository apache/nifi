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

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.parseBoolean;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Map.entry;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.nifi.flow.ScheduledState.ENABLED;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.controller.serialization.FlowSerializationException;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.ControllerServiceAPI;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.minifi.commons.api.MiNiFiProperties;
import org.apache.nifi.properties.ReadableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowEnrichService {

    static final String DEFAULT_SSL_CONTEXT_SERVICE_NAME = "SSL Context Service";

    static final String COMMON_SSL_CONTEXT_SERVICE_NAME = "SSL-Context-Service";
    static final String COMMON_SSL_CONTEXT_SERVICE_ID = "generated-common-ssl-context";
    static final String SITE_TO_SITE_PROVENANCE_REPORTING_TASK_NAME = "Site-To-Site-Provenance-Reporting";
    static final String SITE_TO_SITE_PROVENANCE_REPORTING_TASK_ID = "generated-s2s-provenance-reporting-task";

    private static final Logger LOG = LoggerFactory.getLogger(FlowEnrichService.class);

    private static final String NIFI_BUNDLE_GROUP = "org.apache.nifi";
    private static final String STANDARD_RESTRICTED_SSL_CONTEXT_SERVICE = "org.apache.nifi.ssl.StandardRestrictedSSLContextService";
    private static final String RESTRICTED_SSL_CONTEXT_SERVICE_API = "org.apache.nifi.ssl.RestrictedSSLContextService";
    private static final String SSL_CONTEXT_SERVICE_API = "org.apache.nifi.ssl.SSLContextService";
    private static final String SSL_CONTEXT_SERVICE_NAR = "nifi-ssl-context-service-nar";
    private static final String STANDARD_SERVICES_API_NAR_ARTIFACT = "nifi-standard-services-api-nar";
    private static final String SITE_TO_SITE_PROVENANCE_REPORTING_TASK = "org.apache.nifi.reporting.SiteToSiteProvenanceReportingTask";
    private static final String SITE_TO_SITE_REPORTING_NAR_ARTIFACT = "nifi-site-to-site-reporting-nar";
    private static final String PROVENANCE_REPORTING_TASK_PROTOCOL = "HTTP";
    private static final String PROVENANCE_REPORTING_TASK_BEGINNING_OF_STREAM = "beginning-of-stream";

    private final ReadableProperties minifiProperties;

    public FlowEnrichService(ReadableProperties minifiProperties) {
        this.minifiProperties = minifiProperties;
    }

    public byte[] enrichFlow(byte[] flowCandidate) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enriching flow with content: \n{}", new String(flowCandidate, UTF_8));
        }

        VersionedDataflow versionedDataflow = parseVersionedDataflow(flowCandidate);
        versionedDataflow.setReportingTasks(ofNullable(versionedDataflow.getReportingTasks()).orElseGet(ArrayList::new));
        versionedDataflow.setRegistries(ofNullable(versionedDataflow.getRegistries()).orElseGet(ArrayList::new));
        versionedDataflow.setControllerServices(ofNullable(versionedDataflow.getControllerServices()).orElseGet(ArrayList::new));

        Optional<Integer> maxConcurrentThreads = ofNullable(minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_FLOW_MAX_CONCURRENT_THREADS.getKey()))
            .map(Integer::parseInt);
        maxConcurrentThreads.ifPresent(versionedDataflow::setMaxTimerDrivenThreadCount);

        VersionedProcessGroup rootGroup = versionedDataflow.getRootGroup();
        if (rootGroup.getIdentifier() == null) {
            rootGroup.setIdentifier(randomUUID().toString());
        }
        if (rootGroup.getInstanceIdentifier() == null) {
            rootGroup.setInstanceIdentifier(randomUUID().toString());
        }

        rootGroup.getControllerServices().forEach(controllerService -> controllerService.setScheduledState(ENABLED));

        Optional<VersionedControllerService> commonSslControllerService = createCommonSslControllerService();
        commonSslControllerService.ifPresent(versionedDataflow.getControllerServices()::add);

        commonSslControllerService
            .filter(__ -> parseBoolean(minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_FLOW_USE_PARENT_SSL.getKey())))
            .map(VersionedComponent::getInstanceIdentifier)
            .ifPresent(commonSslControllerServiceInstanceId -> overrideProcessorsSslControllerService(rootGroup, commonSslControllerServiceInstanceId));

        createProvenanceReportingTask(commonSslControllerService.map(VersionedComponent::getInstanceIdentifier).orElse(EMPTY))
            .ifPresent(versionedDataflow.getReportingTasks()::add);
        byte[] enrichedFlow = toByteArray(versionedDataflow);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enriched flow with content: \n{}", new String(enrichedFlow, UTF_8));
        }
        return enrichedFlow;
    }

    private VersionedDataflow parseVersionedDataflow(byte[] flow) {
        try {
            ObjectMapper objectMapper = deserializationObjectMapper();
            return objectMapper.readValue(flow, VersionedDataflow.class);
        } catch (final Exception e) {
            throw new FlowSerializationException("Could not parse flow as a VersionedDataflow", e);
        }
    }

    private ObjectMapper deserializationObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(objectMapper.getTypeFactory()));
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper;
    }

    private Optional<VersionedControllerService> createCommonSslControllerService() {
        if (!parentSslEnabled()) {
            LOG.debug("Parent SSL is disabled, skip creating parent SSL Controller Service");
            return empty();
        }

        LOG.debug("Parent SSL is enabled, creating parent SSL Controller Service");
        VersionedControllerService sslControllerService = new VersionedControllerService();
        sslControllerService.setIdentifier(randomUUID().toString());
        sslControllerService.setInstanceIdentifier(COMMON_SSL_CONTEXT_SERVICE_ID);
        sslControllerService.setName(COMMON_SSL_CONTEXT_SERVICE_NAME);
        sslControllerService.setComments(EMPTY);
        sslControllerService.setType(STANDARD_RESTRICTED_SSL_CONTEXT_SERVICE);
        sslControllerService.setScheduledState(ENABLED);
        sslControllerService.setBulletinLevel(LogLevel.WARN.name());
        sslControllerService.setComponentType(ComponentType.CONTROLLER_SERVICE);
        sslControllerService.setBundle(createBundle(SSL_CONTEXT_SERVICE_NAR));
        sslControllerService.setProperties(sslControllerServiceProperties());
        sslControllerService.setControllerServiceApis(List.of(
            controllerServiceAPI(SSL_CONTEXT_SERVICE_API, createBundle(STANDARD_SERVICES_API_NAR_ARTIFACT)),
            controllerServiceAPI(RESTRICTED_SSL_CONTEXT_SERVICE_API, createBundle(STANDARD_SERVICES_API_NAR_ARTIFACT))
        ));
        sslControllerService.setPropertyDescriptors(Map.of());
        return Optional.of(sslControllerService);
    }

    private boolean parentSslEnabled() {
        return MiNiFiProperties.securityPropertyKeys().stream()
            .map(minifiProperties::getProperty)
            .allMatch(StringUtils::isNotBlank);
    }

    private Map<String, String> sslControllerServiceProperties() {
        return Map.of(
            "Keystore Filename", minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_SECURITY_KEYSTORE.getKey()),
            "Keystore Password", minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_SECURITY_KEYSTORE_PASSWD.getKey()),
            "key-password", minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_SECURITY_KEY_PASSWD.getKey()),
            "Keystore Type", minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_SECURITY_KEYSTORE_TYPE.getKey()),
            "Truststore Filename", minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_SECURITY_TRUSTSTORE.getKey()),
            "Truststore Password", minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_SECURITY_TRUSTSTORE_PASSWD.getKey()),
            "Truststore Type", minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_SECURITY_TRUSTSTORE_TYPE.getKey()),
            "SSL Protocol", minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_SECURITY_SSL_PROTOCOL.getKey())
        );
    }

    private ControllerServiceAPI controllerServiceAPI(String type, Bundle bundle) {
        ControllerServiceAPI controllerServiceAPI = new ControllerServiceAPI();
        controllerServiceAPI.setType(type);
        controllerServiceAPI.setBundle(bundle);
        return controllerServiceAPI;
    }

    private void overrideProcessorsSslControllerService(VersionedProcessGroup processGroup, String commonSslControllerServiceInstanceId) {
        LOG.debug("Use parent SSL is enabled, overriding processors' SSL Controller service to {}", commonSslControllerServiceInstanceId);
        processGroup.getProcessors()
            .forEach(processor -> processor.getProperties()
                .replace(
                    DEFAULT_SSL_CONTEXT_SERVICE_NAME,
                    processor.getProperties().get(DEFAULT_SSL_CONTEXT_SERVICE_NAME),
                    commonSslControllerServiceInstanceId));
        processGroup.getProcessGroups()
            .forEach(childProcessGroup -> overrideProcessorsSslControllerService(childProcessGroup, commonSslControllerServiceInstanceId));
    }

    private Optional<VersionedReportingTask> createProvenanceReportingTask(String sslControllerServiceIdentifier) {
        if (!provenanceReportingEnabled()) {
            LOG.debug("Provenance reporting task is disabled, skip creating provenance reporting task");
            return empty();
        }
        LOG.debug("Provenance reporting task is enabled, creating provenance reporting task");

        VersionedReportingTask reportingTask = new VersionedReportingTask();
        reportingTask.setIdentifier(randomUUID().toString());
        reportingTask.setInstanceIdentifier(SITE_TO_SITE_PROVENANCE_REPORTING_TASK_ID);
        reportingTask.setName(SITE_TO_SITE_PROVENANCE_REPORTING_TASK_NAME);
        reportingTask.setComments(minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_COMMENT.getKey()));
        reportingTask.setType(SITE_TO_SITE_PROVENANCE_REPORTING_TASK);
        reportingTask.setBundle(createBundle(SITE_TO_SITE_REPORTING_NAR_ARTIFACT));
        reportingTask.setScheduledState(ScheduledState.RUNNING);
        reportingTask.setSchedulingStrategy(minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_SCHEDULING_STRATEGY.getKey()));
        reportingTask.setSchedulingPeriod(minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_SCHEDULING_PERIOD.getKey()));
        reportingTask.setComponentType(ComponentType.REPORTING_TASK);
        reportingTask.setProperties(provenanceReportingTaskProperties(sslControllerServiceIdentifier));
        reportingTask.setPropertyDescriptors(Map.of());
        return Optional.of(reportingTask);
    }

    private boolean provenanceReportingEnabled() {
        return MiNiFiProperties.provenanceReportingPropertyKeys().stream()
            .map(minifiProperties::getProperty)
            .allMatch(StringUtils::isNotBlank);
    }

    private Bundle createBundle(String artifact) {
        Bundle bundle = new Bundle();
        bundle.setGroup(NIFI_BUNDLE_GROUP);
        bundle.setArtifact(artifact);
        bundle.setVersion(EMPTY);
        return bundle;
    }

    private Map<String, String> provenanceReportingTaskProperties(String sslControllerServiceIdentifier) {
        return List.of(
                entry("Input Port Name", minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_INPUT_PORT_NAME.getKey())),
                entry("s2s-transport-protocol", PROVENANCE_REPORTING_TASK_PROTOCOL),
                entry("Platform", "nifi"),
                entry("Destination URL", minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_DESTINATION_URL.getKey())),
                entry("include-null-values", FALSE.toString()),
                entry("Compress Events", minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_COMPRESS_EVENTS.getKey())),
                entry("Batch Size", minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_BATCH_SIZE.getKey())),
                entry("Communications Timeout", minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_COMMUNICATIONS_TIMEOUT.getKey())),
                entry("start-position", PROVENANCE_REPORTING_TASK_BEGINNING_OF_STREAM),
                entry("Instance URL", minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_INSTANCE_URL.getKey())),
                entry("SSL Context Service", sslControllerServiceIdentifier))
            .stream()
            .filter(entry -> StringUtils.isNotBlank(entry.getValue()))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private byte[] toByteArray(VersionedDataflow versionedDataflow) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
            JsonFactory factory = new JsonFactory();
            JsonGenerator generator = factory.createGenerator(byteArrayOutputStream);
            generator.setCodec(serializationObjectMapper());
            generator.writeObject(versionedDataflow);
            generator.flush();
            byteArrayOutputStream.flush();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Unable to convert flow to byte array", e);
        }
    }

    private ObjectMapper serializationObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL));
        objectMapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(objectMapper.getTypeFactory()));
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper;
    }
}
