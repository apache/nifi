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
package org.apache.nifi.toolkit.cli.impl.command.nifi.processors;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.ProcessorsResult;
import org.apache.nifi.toolkit.client.FlowClient;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.toolkit.client.ProcessorClient;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorsEntity;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Command to update the version of processors
 */
public class ChangeVersionProcessor extends AbstractNiFiCommand<ProcessorsResult> {

    public ChangeVersionProcessor() {
        super("change-version-processor", ProcessorsResult.class);
    }

    @Override
    public String getDescription() {
        return "Recursively changes the version of the instances of the specified processor. If the process group is specified, the changes "
                + "will be scoped to that process group and its childs ; if not specified the changes will recursively apply to the root process "
                + "group. If the source version is specified, only instances with this version will be updated to the new version.";
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.PG_ID.createOption());
        addOption(CommandOption.EXT_BUNDLE_GROUP.createOption());
        addOption(CommandOption.EXT_BUNDLE_ARTIFACT.createOption());
        addOption(CommandOption.EXT_BUNDLE_VERSION.createOption());
        addOption(CommandOption.EXT_QUALIFIED_NAME.createOption());
        addOption(CommandOption.EXT_BUNDLE_CURRENT_VERSION.createOption());
    }

    @Override
    public ProcessorsResult doExecute(NiFiClient client, Properties properties)
            throws NiFiClientException, IOException, MissingOptionException, CommandException {

        final String bundleGroup = getRequiredArg(properties, CommandOption.EXT_BUNDLE_GROUP);
        final String bundleArtifact = getRequiredArg(properties, CommandOption.EXT_BUNDLE_ARTIFACT);
        final String bundleVersion = getRequiredArg(properties, CommandOption.EXT_BUNDLE_VERSION);
        final String qualifiedName = getRequiredArg(properties, CommandOption.EXT_QUALIFIED_NAME);
        final String sourceVersion = getArg(properties, CommandOption.EXT_BUNDLE_CURRENT_VERSION);

        final FlowClient flowClient = client.getFlowClient();
        final ProcessorClient processorClient = client.getProcessorClient();

        String pgId = getArg(properties, CommandOption.PG_ID);
        if (StringUtils.isBlank(pgId)) {
            pgId = flowClient.getRootGroupId();
        }

        Set<ProcessorEntity> updatedComponents = recursivelyChangeVersionProcessor(flowClient, processorClient, pgId, bundleGroup,
                bundleArtifact, bundleVersion, sourceVersion, qualifiedName);
        ProcessorsEntity processorsEntity = new ProcessorsEntity();
        processorsEntity.setProcessors(updatedComponents);

        return new ProcessorsResult(getResultType(properties), processorsEntity);
    }

    private Set<ProcessorEntity> recursivelyChangeVersionProcessor(FlowClient flowClient, ProcessorClient processorClient, String pgId, String bundleGroup,
            String bundleArtifact, String bundleVersion, String sourceVersion, String qualifiedName) throws NiFiClientException, IOException {

        Set<ProcessorEntity> updatedComponents = new HashSet<>();

        final ProcessGroupFlowEntity sourcePgEntity = flowClient.getProcessGroup(pgId);
        final ProcessGroupFlowDTO flow = sourcePgEntity.getProcessGroupFlow();

        final Set<ProcessorEntity> processors = flow.getFlow().getProcessors();
        for (ProcessorEntity processor : processors) {
            final BundleDTO bundle = processor.getComponent().getBundle();
            if (bundle.getGroup().equals(bundleGroup)
                    && bundle.getArtifact().equals(bundleArtifact)
                    && processor.getComponent().getType().equals(qualifiedName)
                    && (StringUtils.isBlank(sourceVersion) || bundle.getVersion().equals(sourceVersion))) {

                final boolean isRunning = processor.getComponent().getState().equals("RUNNING");
                if (isRunning) {
                    // processor needs to be stopped for changing the version
                    processorClient.stopProcessor(processor);
                    // get the updated entity to have the correct revision
                    processor = processorClient.getProcessor(processor.getId());
                }

                final BundleDTO updatedBundle = new BundleDTO(bundleGroup, bundleArtifact, bundleVersion);
                final ProcessorDTO processorDto = new ProcessorDTO();
                processorDto.setId(processor.getId());
                processorDto.setBundle(updatedBundle);

                final ProcessorEntity updatedEntity = new ProcessorEntity();
                updatedEntity.setRevision(processor.getRevision());
                updatedEntity.setComponent(processorDto);
                updatedEntity.setId(processor.getId());

                processorClient.updateProcessor(updatedEntity);

                if (isRunning) { // restart the component that was previously running
                    // get the updated entity to have the correct revision
                    processor = processorClient.getProcessor(processor.getId());
                    processorClient.startProcessor(processor);
                }

                // get latest version of the entity
                processor = processorClient.getProcessor(processor.getId());
                updatedComponents.add(processor);
            }
        }

        final Set<ProcessGroupEntity> processGroups = flow.getFlow().getProcessGroups();
        for (ProcessGroupEntity processGroup : processGroups) {
            updatedComponents.addAll(recursivelyChangeVersionProcessor(flowClient, processorClient, processGroup.getId(), bundleGroup,
                    bundleArtifact, bundleVersion, sourceVersion, qualifiedName));
        }

        return updatedComponents;
    }
}
