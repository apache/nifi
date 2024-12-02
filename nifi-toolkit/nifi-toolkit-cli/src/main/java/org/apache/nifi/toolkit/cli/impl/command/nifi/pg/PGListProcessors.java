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
package org.apache.nifi.toolkit.cli.impl.command.nifi.pg;

import org.apache.commons.cli.MissingOptionException;
import org.apache.nifi.toolkit.cli.api.CommandException;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.nifi.ProcessorsResult;
import org.apache.nifi.toolkit.client.NiFiClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ProcessorsEntity;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Command to recursively list processors in a process group
 */
public class PGListProcessors extends AbstractNiFiCommand<ProcessorsResult> {

    public PGListProcessors() {
        super("pg-list-processors", ProcessorsResult.class);
    }

    @Override
    public String getDescription() {
        return "Returns the list of processors for the specified process group. Listing can be scoped specific processors (source/destination)";
    }

    @Override
    protected void doInitialize(Context context) {
        addOption(CommandOption.PG_ID.createOption());
        addOption(CommandOption.FILTER.createOption());
    }

    @Override
    public ProcessorsResult doExecute(final NiFiClient client, final Properties properties)
            throws NiFiClientException, IOException, CommandException, MissingOptionException {
        final String pgId = getRequiredArg(properties, CommandOption.PG_ID);
        final String filter = getArg(properties, CommandOption.FILTER);

        if (filter != null && !(filter.equalsIgnoreCase("source") || filter.equalsIgnoreCase("destination"))) {
            throw new CommandException("Filter must be either 'source' or 'destination'");
        }

        ProcessGroupFlowEntity entity = client.getFlowClient().getProcessGroup(pgId);
        final FlowDTO flow = entity.getProcessGroupFlow().getFlow();
        final Set<ProcessorEntity> processors = new HashSet<>(getProcessors(client, flow, filter));

        ProcessorsEntity processorsEntity = new ProcessorsEntity();
        processorsEntity.setProcessors(processors);

        return new ProcessorsResult(getResultType(properties), processorsEntity);
    }

    private Set<ProcessorEntity> getProcessors(final NiFiClient client, final FlowDTO flow, final String filter) throws NiFiClientException, IOException {
        final Set<ProcessorEntity> processors = new HashSet<>();
        for (ProcessGroupEntity pg : flow.getProcessGroups()) {
            processors.addAll(getProcessors(client, pg, filter));
        }
        for (ProcessorEntity processor : flow.getProcessors()) {
            addProcessor(processors, flow, processor, filter);
        }
        return processors;
    }

    private Set<ProcessorEntity> getProcessors(final NiFiClient client, final ProcessGroupEntity pg, final String filter) throws NiFiClientException, IOException {
        final ProcessGroupFlowEntity entity = client.getFlowClient().getProcessGroup(pg.getId());
        final FlowDTO flow = entity.getProcessGroupFlow().getFlow();
        return getProcessors(client, flow, filter);
    }

    private void addProcessor(final Set<ProcessorEntity> listProcessors, final FlowDTO flow, final ProcessorEntity processor, final String filter) {
        if (filter != null) {
            if ("source".equalsIgnoreCase(filter) && getInputRelNotSelfCount(flow, processor) == 0 && getOutputRelNotSelfCount(flow, processor) > 0) {
                listProcessors.add(processor);
            } else if ("destination".equalsIgnoreCase(filter) && getOutputRelNotSelfCount(flow, processor) == 0 && getInputRelNotSelfCount(flow, processor) > 0) {
                listProcessors.add(processor);
            }
        } else {
            listProcessors.add(processor);
        }
    }

    private long getInputRelNotSelfCount(final FlowDTO flow, final ProcessorEntity processor) {
        return flow.getConnections()
                .stream()
                .filter(c -> c.getComponent().getDestination().getId().equals(processor.getId())
                        && !c.getComponent().getSource().getId().equals(processor.getId()))
                .count();
    }

    private long getOutputRelNotSelfCount(final FlowDTO flow, final ProcessorEntity processor) {
        return flow.getConnections()
                .stream()
                .filter(c -> c.getComponent().getSource().getId().equals(processor.getId())
                        && !c.getComponent().getDestination().getId().equals(processor.getId()))
                .count();
    }

}
