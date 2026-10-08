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
package org.apache.nifi.toolkit.cli.impl.command.nifi.connectors;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.ConnectorsResult;
import org.apache.nifi.toolkit.client.ConnectorClient;
import org.apache.nifi.toolkit.client.FlowClient;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ConnectorsEntity;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Command to update the NAR version of Connector instances.
 */
public class ChangeVersionConnector extends AbstractNiFiCommand<ConnectorsResult> {

    public ChangeVersionConnector() {
        super("change-version-connector", ConnectorsResult.class);
    }

    @Override
    public String getDescription() {
        return "Changes the version of Connector instances of the specified type. If the current version is specified, only instances "
                + "with that version are updated. Running Connectors are stopped before the version is changed and restarted afterward.";
    }

    @Override
    protected void doInitialize(final Context context) {
        addOption(CommandOption.EXT_BUNDLE_GROUP.createOption());
        addOption(CommandOption.EXT_BUNDLE_ARTIFACT.createOption());
        addOption(CommandOption.EXT_BUNDLE_VERSION.createOption());
        addOption(CommandOption.EXT_QUALIFIED_NAME.createOption());
        addOption(CommandOption.EXT_BUNDLE_CURRENT_VERSION.createOption());
    }

    @Override
    public ConnectorsResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final String bundleGroup = getRequiredArg(properties, CommandOption.EXT_BUNDLE_GROUP);
        final String bundleArtifact = getRequiredArg(properties, CommandOption.EXT_BUNDLE_ARTIFACT);
        final String bundleVersion = getRequiredArg(properties, CommandOption.EXT_BUNDLE_VERSION);
        final String qualifiedName = getRequiredArg(properties, CommandOption.EXT_QUALIFIED_NAME);
        final String sourceVersion = getArg(properties, CommandOption.EXT_BUNDLE_CURRENT_VERSION);

        final FlowClient flowClient = client.getFlowClient();
        final ConnectorClient connectorClient = client.getConnectorClient();
        final ConnectorsEntity connectorsEntity = flowClient.getConnectors();
        final Set<ConnectorEntity> updatedComponents = new HashSet<>();

        if (connectorsEntity.getConnectors() != null) {
            for (final ConnectorEntity connector : connectorsEntity.getConnectors()) {
                if (isUnreadable(connector)) {
                    throw new CommandException("Cannot change version because Connector " + connector.getId() + " is unreadable");
                }
            }

            for (final ConnectorEntity connector : connectorsEntity.getConnectors()) {
                final BundleDTO bundle = connector.getComponent().getBundle();
                if (!bundle.getGroup().equals(bundleGroup)
                        || !bundle.getArtifact().equals(bundleArtifact)
                        || !connector.getComponent().getType().equals(qualifiedName)
                        || (!StringUtils.isBlank(sourceVersion) && !bundle.getVersion().equals(sourceVersion))) {
                    continue;
                }

                if (bundleVersion.equals(bundle.getVersion())) {
                    continue;
                }

                final String currentState = connector.getComponent().getState();
                if ("TROUBLESHOOTING".equals(currentState)) {
                    throw new CommandException("Cannot change version of Connector " + connector.getId() + " while it is in Troubleshooting");
                }

                final boolean isRunning = "RUNNING".equals(currentState) || "STARTING".equals(currentState);
                if (isRunning) {
                    connectorClient.stopConnector(connector);
                }

                try {
                    final boolean shouldWaitForReloadable = isRunning || !isReloadableState(currentState);
                    final ConnectorEntity reloadableConnector;
                    if (shouldWaitForReloadable) {
                        reloadableConnector = waitForConnectorReloadable(connectorClient, connector.getId());
                    } else {
                        reloadableConnector = connector;
                    }

                    final BundleDTO updatedBundle = new BundleDTO(bundleGroup, bundleArtifact, bundleVersion);
                    final ConnectorDTO connectorDto = new ConnectorDTO();
                    connectorDto.setId(reloadableConnector.getId());
                    connectorDto.setBundle(updatedBundle);

                    final ConnectorEntity updatedEntity = new ConnectorEntity();
                    updatedEntity.setRevision(reloadableConnector.getRevision());
                    updatedEntity.setComponent(connectorDto);
                    updatedEntity.setId(reloadableConnector.getId());

                    connectorClient.updateConnector(updatedEntity);
                } catch (final NiFiClientException | IOException | CommandException changeVersionFailure) {
                    if (isRunning) {
                        try {
                            final ConnectorEntity currentConnector = connectorClient.getConnector(connector.getId());
                            connectorClient.startConnector(currentConnector);
                        } catch (final Exception restartFailure) {
                            changeVersionFailure.addSuppressed(restartFailure);
                        }
                    }

                    throw changeVersionFailure;
                }

                if (isRunning) {
                    final ConnectorEntity connectorToStart = connectorClient.getConnector(connector.getId());
                    connectorClient.startConnector(connectorToStart);
                }

                final ConnectorEntity updatedConnector = connectorClient.getConnector(connector.getId());
                updatedComponents.add(updatedConnector);
            }
        }

        final ConnectorsEntity resultEntity = new ConnectorsEntity();
        resultEntity.setConnectors(updatedComponents);
        return new ConnectorsResult(getResultType(properties), resultEntity);
    }

    private boolean isUnreadable(final ConnectorEntity connector) {
        if (connector.getComponent() == null) {
            return true;
        }

        final PermissionsDTO permissions = connector.getPermissions();
        if (permissions == null) {
            return false;
        }

        return Boolean.FALSE.equals(permissions.getCanRead());
    }

    private ConnectorEntity waitForConnectorReloadable(final ConnectorClient connectorClient, final String connectorId)
            throws NiFiClientException, IOException, CommandException {
        final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60);
        ConnectorEntity connector = connectorClient.getConnector(connectorId);
        while (System.currentTimeMillis() < deadline) {
            final String state = connector.getComponent().getState();
            if (isReloadableState(state)) {
                return connector;
            }

            try {
                Thread.sleep(200L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CommandException("Interrupted while waiting for Connector " + connectorId + " to stop");
            }

            connector = connectorClient.getConnector(connectorId);
        }

        throw new CommandException("Timed out waiting for Connector " + connectorId + " to stop; current state is "
                + connector.getComponent().getState());
    }

    private boolean isReloadableState(final String state) {
        return "STOPPED".equals(state) || "UPDATED".equals(state) || "UPDATE_FAILED".equals(state);
    }
}
