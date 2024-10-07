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
import static java.util.Map.entry;
import static java.util.Objects.isNull;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.nifi.flow.ScheduledState.ENABLED;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ControllerServiceAPI;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.minifi.commons.api.MiNiFiProperties;
import org.apache.nifi.properties.ReadableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardFlowEnrichService implements FlowEnrichService {

    static final String DEFAULT_SSL_CONTEXT_SERVICE_NAME = "SSL Context Service";

    static final String PARENT_SSL_CONTEXT_SERVICE_NAME = "SSL-Context-Service";
    static final String PARENT_SSL_CONTEXT_SERVICE_ID = "generated-common-ssl-context";
    static final String SITE_TO_SITE_PROVENANCE_REPORTING_TASK_NAME = "Site-To-Site-Provenance-Reporting";
    static final String SITE_TO_SITE_PROVENANCE_REPORTING_TASK_ID = "generated-s2s-provenance-reporting-task";

    private static final Logger LOG = LoggerFactory.getLogger(StandardFlowEnrichService.class);

    private static final String DEFAULT_PARAMETER_CONTEXT = "default-parameter-context";
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
    private static final String DEFAULT_BULLETIN_LEVEL = "WARN";
    private static final String DEFAULT_EXECUTION_NODE = "ALL";
    private static final Position DEFAULT_POSITION = new Position(0.0, 0.0);
    private static final Predicate<? super VersionedComponent> IS_LEGACY_COMPONENT = versionedComponent -> isBlank(versionedComponent.getInstanceIdentifier());

    private final ReadableProperties minifiProperties;

    public StandardFlowEnrichService(ReadableProperties minifiProperties) {
        this.minifiProperties = minifiProperties;
    }

    @Override
    public VersionedDataflow enrichFlow(VersionedDataflow versionedDataflow) {
        versionedDataflow.setReportingTasks(ofNullable(versionedDataflow.getReportingTasks()).orElseGet(ArrayList::new));
        versionedDataflow.setRegistries(ofNullable(versionedDataflow.getRegistries()).orElseGet(ArrayList::new));
        versionedDataflow.setControllerServices(ofNullable(versionedDataflow.getControllerServices()).orElseGet(ArrayList::new));
        versionedDataflow.setParameterContexts(
            ofNullable(versionedDataflow.getParameterContexts()).orElseGet(ArrayList::new)
                .stream()
                .map(versionedParameterContext -> {
                    versionedParameterContext.setIdentifier(ofNullable(versionedParameterContext.getIdentifier()).orElseGet(randomUUID()::toString));
                    versionedParameterContext.setInstanceIdentifier(ofNullable(versionedParameterContext.getInstanceIdentifier()).orElseGet(randomUUID()::toString));
                    return versionedParameterContext;
                })
                .collect(toList())
        );

        Optional<Integer> maxConcurrentThreads = ofNullable(minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_FLOW_MAX_CONCURRENT_THREADS.getKey()))
            .map(Integer::parseInt);
        maxConcurrentThreads.ifPresent(versionedDataflow::setMaxTimerDrivenThreadCount);

        VersionedProcessGroup rootGroup = versionedDataflow.getRootGroup();
        if (isBlank(rootGroup.getIdentifier())) {
            rootGroup.setIdentifier(randomUUID().toString());
        }
        if (isNull(rootGroup.getPosition())) {
            rootGroup.setPosition(DEFAULT_POSITION);
        }

        enableControllerServices(rootGroup);

        Optional<VersionedControllerService> parentSslControllerService = createParentSslControllerService();
        parentSslControllerService.ifPresent(versionedDataflow.getRootGroup().getControllerServices()::add);

        parentSslControllerService
            .filter(__ -> isOverrideSslContextInComponentsSet())
            .map(VersionedComponent::getInstanceIdentifier)
            .ifPresent(parentSslControllerServiceInstanceId -> {
                overrideComponentsSslControllerService(rootGroup, parentSslControllerServiceInstanceId);
                deleteUnusedSslControllerServices(rootGroup);
            });

        createProvenanceReportingTask(parentSslControllerService).ifPresent(versionedDataflow.getReportingTasks()::add);

        createDefaultParameterContext(versionedDataflow);

        if (IS_LEGACY_COMPONENT.test(rootGroup)) {
            LOG.info("Legacy flow detected. Initializing missing but mandatory properties on components");
            initializeComponentsMissingProperties(rootGroup);
            Map<String, String> idToInstanceIdMap = createIdToInstanceIdMap(rootGroup);
            setConnectableComponentsInstanceId(rootGroup, idToInstanceIdMap);
        }

        return versionedDataflow;
    }

    private void createDefaultParameterContext(VersionedDataflow versionedDataflow) {
        VersionedParameterContext versionedParameterContext = new VersionedParameterContext();
        versionedParameterContext.setIdentifier(DEFAULT_PARAMETER_CONTEXT);
        versionedParameterContext.setInstanceIdentifier(DEFAULT_PARAMETER_CONTEXT);
        versionedParameterContext.setName(DEFAULT_PARAMETER_CONTEXT);
        versionedParameterContext.setDescription(DEFAULT_PARAMETER_CONTEXT);
        versionedParameterContext.setParameters(new HashSet<>());

        ofNullable(versionedDataflow.getParameterContexts())
            .ifPresentOrElse(
                versionedParameterContexts -> versionedParameterContexts.add(versionedParameterContext),
                () -> {
                    List<VersionedParameterContext> parameterContexts = new ArrayList<>();
                    parameterContexts.add(versionedParameterContext);
                    versionedDataflow.setParameterContexts(parameterContexts);
                }
            );
    }

    private void enableControllerServices(VersionedProcessGroup processGroup) {
        ofNullable(processGroup.getControllerServices()).orElseGet(Set::of)
            .forEach(controllerService -> controllerService.setScheduledState(ENABLED));
        ofNullable(processGroup.getProcessGroups()).orElseGet(Set::of)
            .forEach(this::enableControllerServices);
    }

    private Optional<VersionedControllerService> createParentSslControllerService() {
        if (!parentSslContextIsEnabled()) {
            LOG.debug("Parent SSL is disabled, skip creating parent SSL Controller Service");
            return empty();
        }

        LOG.debug("Parent SSL is enabled, creating parent SSL Controller Service");
        VersionedControllerService sslControllerService = new VersionedControllerService();
        sslControllerService.setIdentifier(PARENT_SSL_CONTEXT_SERVICE_ID);
        sslControllerService.setInstanceIdentifier(PARENT_SSL_CONTEXT_SERVICE_ID);
        sslControllerService.setName(PARENT_SSL_CONTEXT_SERVICE_NAME);
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

    private boolean parentSslContextIsEnabled() {
        return MiNiFiProperties.securityPropertyKeys().stream()
            .map(minifiProperties::getProperty)
            .allMatch(StringUtils::isNotBlank);
    }

    private boolean isOverrideSslContextInComponentsSet() {
        return parseBoolean(minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_FLOW_USE_PARENT_SSL.getKey()));
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

    private void overrideComponentsSslControllerService(VersionedProcessGroup processGroup, String commonSslControllerServiceInstanceId) {
        LOG.debug("Use parent SSL is enabled, overriding processors' and controller services' SSL Controller service property to {}", commonSslControllerServiceInstanceId);
        Consumer<VersionedConfigurableExtension> updateControllerServicesId =
            extension -> replaceProperty(extension, DEFAULT_SSL_CONTEXT_SERVICE_NAME, commonSslControllerServiceInstanceId);
        processGroup.getProcessors()
            .forEach(updateControllerServicesId);
        processGroup.getControllerServices()
            .stream()
            .filter(controllerService -> !controllerService.getInstanceIdentifier().equals(PARENT_SSL_CONTEXT_SERVICE_ID))
            .forEach(updateControllerServicesId);
        processGroup.getProcessGroups()
            .forEach(childProcessGroup -> overrideComponentsSslControllerService(childProcessGroup, commonSslControllerServiceInstanceId));
    }

    private void deleteUnusedSslControllerServices(VersionedProcessGroup processGroup) {
        Set<VersionedControllerService> controllerServicesWithoutUnusedSslContextServices = processGroup.getControllerServices()
            .stream()
            .filter(controllerService -> !controllerService.getControllerServiceApis().stream().map(ControllerServiceAPI::getType).anyMatch(SSL_CONTEXT_SERVICE_API::equals)
                || controllerService.getInstanceIdentifier().equals(PARENT_SSL_CONTEXT_SERVICE_ID))
            .collect(toSet());
        processGroup.setControllerServices(controllerServicesWithoutUnusedSslContextServices);
        processGroup.getProcessGroups().forEach(this::deleteUnusedSslControllerServices);
    }

    private void replaceProperty(VersionedConfigurableExtension extension, String key, String newValue) {
        String oldValue = extension.getProperties().get(key);
        extension.getProperties().replace(key, oldValue, newValue);
    }

    private Optional<VersionedReportingTask> createProvenanceReportingTask(Optional<VersionedControllerService> commonSslControllerService) {
        if (!provenanceReportingEnabled()) {
            LOG.debug("Provenance reporting task is disabled, skip creating provenance reporting task");
            return empty();
        }
        LOG.debug("Provenance reporting task is enabled, creating provenance reporting task");

        VersionedReportingTask reportingTask = new VersionedReportingTask();
        reportingTask.setIdentifier(SITE_TO_SITE_PROVENANCE_REPORTING_TASK_ID);
        reportingTask.setInstanceIdentifier(SITE_TO_SITE_PROVENANCE_REPORTING_TASK_ID);
        reportingTask.setName(SITE_TO_SITE_PROVENANCE_REPORTING_TASK_NAME);
        reportingTask.setComments(minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_COMMENT.getKey()));
        reportingTask.setType(SITE_TO_SITE_PROVENANCE_REPORTING_TASK);
        reportingTask.setBundle(createBundle(SITE_TO_SITE_REPORTING_NAR_ARTIFACT));
        reportingTask.setScheduledState(ScheduledState.RUNNING);
        reportingTask.setSchedulingStrategy(minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_SCHEDULING_STRATEGY.getKey()));
        reportingTask.setSchedulingPeriod(minifiProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_PROVENANCE_REPORTING_SCHEDULING_PERIOD.getKey()));
        reportingTask.setComponentType(ComponentType.REPORTING_TASK);
        reportingTask.setProperties(provenanceReportingTaskProperties(commonSslControllerService.map(VersionedComponent::getInstanceIdentifier).orElse(EMPTY)));
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
            .collect(toMap(Entry::getKey, Entry::getValue));
    }

    private void initializeComponentsMissingProperties(VersionedProcessGroup versionedProcessGroup) {
        versionedProcessGroup.setInstanceIdentifier(randomUUID().toString());

        Stream.of(
                ofNullable(versionedProcessGroup.getControllerServices()).orElse(Set.of()),
                ofNullable(versionedProcessGroup.getConnections()).orElse(Set.of()),
                ofNullable(versionedProcessGroup.getProcessors()).orElse(Set.of()),
                ofNullable(versionedProcessGroup.getInputPorts()).orElse(Set.of()),
                ofNullable(versionedProcessGroup.getOutputPorts()).orElse(Set.of()),
                ofNullable(versionedProcessGroup.getFunnels()).orElse(Set.of()),
                ofNullable(versionedProcessGroup.getRemoteProcessGroups()).orElse(Set.of()),
                ofNullable(versionedProcessGroup.getRemoteProcessGroups()).orElse(Set.of())
                    .stream()
                    .map(VersionedRemoteProcessGroup::getInputPorts)
                    .flatMap(Set::stream)
                    .collect(toSet()),
                ofNullable(versionedProcessGroup.getRemoteProcessGroups()).orElse(Set.of())
                    .stream()
                    .map(VersionedRemoteProcessGroup::getOutputPorts)
                    .flatMap(Set::stream)
                    .collect(toSet()))
            .flatMap(Set::stream)
            .filter(IS_LEGACY_COMPONENT)
            .forEach(versionedComponent -> {
                versionedComponent.setInstanceIdentifier(randomUUID().toString());
                if (versionedComponent instanceof VersionedProcessor processor) {
                    if (isBlank(processor.getBulletinLevel())) {
                        processor.setBulletinLevel(DEFAULT_BULLETIN_LEVEL);
                    }
                    if (isBlank(processor.getExecutionNode())) {
                        processor.setExecutionNode(DEFAULT_EXECUTION_NODE);
                    }
                }
            });

        versionedProcessGroup.getProcessGroups().forEach(this::initializeComponentsMissingProperties);
    }

    private Map<String, String> createIdToInstanceIdMap(VersionedProcessGroup versionedProcessGroup) {
        Map<String, String> thisProcessGroupIdToInstanceIdMaps = Stream.of(
                ofNullable(versionedProcessGroup.getProcessors()).orElse(Set.of()),
                ofNullable(versionedProcessGroup.getInputPorts()).orElse(Set.of()),
                ofNullable(versionedProcessGroup.getOutputPorts()).orElse(Set.of()),
                ofNullable(versionedProcessGroup.getFunnels()).orElse(Set.of()),
                ofNullable(versionedProcessGroup.getRemoteProcessGroups()).orElse(Set.of())
                    .stream()
                    .map(VersionedRemoteProcessGroup::getInputPorts)
                    .flatMap(Set::stream)
                    .collect(toSet()),
                ofNullable(versionedProcessGroup.getRemoteProcessGroups()).orElse(Set.of())
                    .stream()
                    .map(VersionedRemoteProcessGroup::getOutputPorts)
                    .flatMap(Set::stream)
                    .collect(toSet())
            )
            .flatMap(Set::stream)
            .collect(toMap(VersionedComponent::getIdentifier, VersionedComponent::getInstanceIdentifier));

        Stream<Map<String, String>> childProcessGroupsIdToInstanceIdMaps = ofNullable(versionedProcessGroup.getProcessGroups()).orElse(Set.of())
            .stream()
            .map(this::createIdToInstanceIdMap);

        return Stream.concat(
                Stream.of(thisProcessGroupIdToInstanceIdMaps),
                childProcessGroupsIdToInstanceIdMaps)
            .map(Map::entrySet)
            .flatMap(Set::stream)
            .collect(toMap(Entry::getKey, Entry::getValue));
    }

    private void setConnectableComponentsInstanceId(VersionedProcessGroup versionedProcessGroup, Map<String, String> idToInstanceIdMap) {
        ofNullable(versionedProcessGroup.getConnections()).orElse(Set.of())
            .forEach(connection -> {
                ConnectableComponent source = connection.getSource();
                source.setInstanceIdentifier(idToInstanceIdMap.get(source.getId()));
                ConnectableComponent destination = connection.getDestination();
                destination.setInstanceIdentifier(idToInstanceIdMap.get(destination.getId()));
            });
        ofNullable(versionedProcessGroup.getProcessGroups()).orElse(Set.of())
            .forEach(childProcessGroup -> setConnectableComponentsInstanceId(childProcessGroup, idToInstanceIdMap));
    }
}
