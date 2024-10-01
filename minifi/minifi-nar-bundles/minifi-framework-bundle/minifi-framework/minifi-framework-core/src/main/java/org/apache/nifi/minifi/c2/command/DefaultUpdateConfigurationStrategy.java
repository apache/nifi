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

package org.apache.nifi.minifi.c2.command;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static java.util.UUID.randomUUID;
import static java.util.function.Predicate.not;
import static org.apache.nifi.minifi.commons.api.MiNiFiConstants.BACKUP_EXTENSION;
import static org.apache.nifi.minifi.commons.api.MiNiFiConstants.RAW_EXTENSION;
import static org.apache.nifi.minifi.commons.util.FlowUpdateUtils.backup;
import static org.apache.nifi.minifi.commons.util.FlowUpdateUtils.persist;
import static org.apache.nifi.minifi.commons.util.FlowUpdateUtils.removeIfExists;
import static org.apache.nifi.minifi.commons.util.FlowUpdateUtils.revert;
import static org.apache.nifi.minifi.commons.utils.RetryUtil.retry;
import static org.apache.nifi.minifi.validator.FlowValidator.validate;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FilenameUtils;
import org.apache.nifi.c2.client.service.operation.UpdateConfigurationStrategy;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.minifi.commons.service.FlowEnrichService;
import org.apache.nifi.minifi.commons.service.FlowPropertyEncryptor;
import org.apache.nifi.minifi.commons.service.FlowSerDeService;
import org.apache.nifi.minifi.validator.ValidationException;
import org.apache.nifi.services.FlowService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultUpdateConfigurationStrategy implements UpdateConfigurationStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultUpdateConfigurationStrategy.class);

    private static final int FLOW_DRAIN_RETRY_PAUSE_DURATION_MS = 1000;
    private static final int FLOW_DRAIN_MAX_RETRIES = 60;

    private final FlowController flowController;
    private final FlowService flowService;
    private final FlowEnrichService flowEnrichService;
    private final FlowPropertyEncryptor flowPropertyEncryptor;
    private final FlowSerDeService flowSerDeService;
    private final Path flowConfigurationFile;
    private final Path backupFlowConfigurationFile;
    private final Path rawFlowConfigurationFile;
    private final Path backupRawFlowConfigurationFile;

    public DefaultUpdateConfigurationStrategy(FlowController flowController, FlowService flowService, FlowEnrichService flowEnrichService,
                                              FlowPropertyEncryptor flowPropertyEncryptor, FlowSerDeService flowSerDeService, String flowConfigurationFile) {
        this.flowController = flowController;
        this.flowService = flowService;
        this.flowEnrichService = flowEnrichService;
        this.flowPropertyEncryptor = flowPropertyEncryptor;
        this.flowSerDeService = flowSerDeService;
        Path flowConfigurationFilePath = Path.of(flowConfigurationFile).toAbsolutePath();
        this.flowConfigurationFile = flowConfigurationFilePath;
        this.backupFlowConfigurationFile = Path.of(flowConfigurationFilePath + BACKUP_EXTENSION);
        String flowConfigurationFileBaseName = FilenameUtils.getBaseName(flowConfigurationFilePath.toString());
        this.rawFlowConfigurationFile = flowConfigurationFilePath.getParent().resolve(flowConfigurationFileBaseName + RAW_EXTENSION);
        this.backupRawFlowConfigurationFile = flowConfigurationFilePath.getParent().resolve(flowConfigurationFileBaseName + BACKUP_EXTENSION + RAW_EXTENSION);
    }

    @Override
    public void update(byte[] rawFlow) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Attempting to update flow with content: \n{}", new String(rawFlow, UTF_8));
        }
        Set<String> originalConnectionIds = emptySet();
        try {
            originalConnectionIds = findAllExistingConnections(flowController.getFlowManager().getRootGroup())
                .stream()
                .map(Connection::getIdentifier)
                .collect(Collectors.toSet());
            VersionedDataflow rawDataFlow = flowSerDeService.deserialize(rawFlow);

            VersionedDataflow propertyEncryptedRawDataFlow = flowPropertyEncryptor.encryptSensitiveProperties(rawDataFlow);
            byte[] serializedPropertyEncryptedRawDataFlow = flowSerDeService.serialize(propertyEncryptedRawDataFlow);
            VersionedDataflow enrichedFlowCandidate = flowEnrichService.enrichFlow(propertyEncryptedRawDataFlow);
            byte[] serializedEnrichedFlowCandidate = flowSerDeService.serialize(enrichedFlowCandidate);

            backup(flowConfigurationFile, backupFlowConfigurationFile);
            backup(rawFlowConfigurationFile, backupRawFlowConfigurationFile);

            persist(serializedPropertyEncryptedRawDataFlow, rawFlowConfigurationFile, false);
            persist(serializedEnrichedFlowCandidate, flowConfigurationFile, true);

            reloadFlow(findAllProposedConnectionIds(enrichedFlowCandidate.getRootGroup()));

        } catch (IllegalStateException e) {
            LOGGER.error("Configuration update failed. Reverting and reloading previous flow", e);
            revert(backupFlowConfigurationFile, flowConfigurationFile);
            revert(backupRawFlowConfigurationFile, rawFlowConfigurationFile);
            try {
                reloadFlow(originalConnectionIds);
            } catch (ValidationException ex) {
                LOGGER.error("Unable to reload the reverted flow", ex);
                throw ex;
            } catch (Exception exception) {
                throw new RuntimeException(exception);
            }
            throw e;
        } catch (Exception e) {
            LOGGER.error("Configuration update failed. Reverting to previous flow, no reload is necessary", e);
            revert(backupFlowConfigurationFile, flowConfigurationFile);
            revert(backupRawFlowConfigurationFile, rawFlowConfigurationFile);
            throw new RuntimeException(e);
        } finally {
            removeIfExists(backupFlowConfigurationFile);
            removeIfExists(backupRawFlowConfigurationFile);
        }
    }

    private void reloadFlow(Set<String> proposedConnectionIds) throws IOException {
        LOGGER.info("Initiating flow reload");
        stopFlowGracefully(flowController.getFlowManager().getRootGroup(), proposedConnectionIds);

        flowService.load(null);
        flowController.onFlowInitialized(true);

        List<ValidationResult> validationErrors = validate(flowController.getFlowManager());
        if (!validationErrors.isEmpty()) {
            LOGGER.error("Validation errors found when reloading the flow: {}", validationErrors);
            throw new ValidationException("Unable to start flow due to validation errors", validationErrors);
        }

        flowController.getFlowManager().getRootGroup().startProcessing();
        LOGGER.info("Flow has been reloaded successfully");
    }

    private void stopFlowGracefully(ProcessGroup rootGroup, Set<String> proposedConnectionIds) {
        LOGGER.info("Stopping flow gracefully");
        Optional<ProcessGroup> drainResult = stopSourceProcessorsAndWaitFlowToDrain(rootGroup);

        waitForStopOrLogTimeOut(rootGroup.stopProcessing());
        waitForStopOrLogTimeOut(rootGroup.stopComponents());

        rootGroup.getRemoteProcessGroups().stream()
            .map(RemoteProcessGroup::stopTransmitting)
            .forEach(this::waitForStopOrLogTimeOut);

        drainResult.ifPresentOrElse(
            emptyQueuesForNonReferencedQueues(proposedConnectionIds),
            () -> LOGGER.info("Flow has been stopped gracefully"));
    }

    private Consumer<ProcessGroup> emptyQueuesForNonReferencedQueues(Set<String> proposedConnectionIds) {
        return rootProcessGroup -> {
            LOGGER.warn("Flow did not stop within graceful period. Force stopping flow and emptying non referenced queues");
            findAllExistingConnections(rootProcessGroup).stream()
                .filter(connection -> !proposedConnectionIds.contains(connection.getIdentifier()))
                .map(Connection::getFlowFileQueue)
                .forEach(queue -> queue.dropFlowFiles(randomUUID().toString(), randomUUID().toString()));
        };
    }

    private Optional<ProcessGroup> stopSourceProcessorsAndWaitFlowToDrain(ProcessGroup rootGroup) {
        rootGroup.getProcessors().stream().filter(this::isSourceNode).forEach(rootGroup::stopProcessor);
        return retry(() -> rootGroup, not(ProcessGroup::isDataQueued), FLOW_DRAIN_MAX_RETRIES, FLOW_DRAIN_RETRY_PAUSE_DURATION_MS);
    }

    private boolean isSourceNode(ProcessorNode processorNode) {
        boolean hasNoIncomingConnection = !processorNode.hasIncomingConnection();

        boolean allIncomingConnectionsAreLoopConnections = processorNode.getIncomingConnections()
            .stream()
            .allMatch(connection -> connection.getSource().equals(processorNode));

        return hasNoIncomingConnection || allIncomingConnectionsAreLoopConnections;
    }

    private void waitForStopOrLogTimeOut(Future<?> future) {
        try {
            future.get(10000, TimeUnit.MICROSECONDS);
        } catch (Exception e) {
            LOGGER.warn("Unable to stop component within defined interval", e);
        }
    }

    private Set<String> findAllProposedConnectionIds(VersionedProcessGroup versionedProcessGroup) {
        return versionedProcessGroup == null
            ? emptySet()
            : Stream.concat(
                versionedProcessGroup.getConnections().stream().map(VersionedConnection::getInstanceIdentifier),
                versionedProcessGroup.getProcessGroups().stream().map(this::findAllProposedConnectionIds).flatMap(Set::stream)
            ).collect(Collectors.toSet());
    }

    private Set<Connection> findAllExistingConnections(ProcessGroup processGroup) {
        return processGroup == null
            ? emptySet()
            : Stream.concat(
                processGroup.getConnections().stream(),
                processGroup.getProcessGroups().stream().map(this::findAllExistingConnections).flatMap(Set::stream)
            ).collect(Collectors.toSet());
    }
}
