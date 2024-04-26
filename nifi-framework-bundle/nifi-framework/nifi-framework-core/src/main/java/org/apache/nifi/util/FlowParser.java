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
package org.apache.nifi.util;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * Parses a flow and returns the root group id and root group ports.
 */
public class FlowParser {

    private static final Logger logger = LoggerFactory.getLogger(FlowParser.class);

    /**
     * Extracts the root group id from the flow configuration file provided in nifi.properties, and extracts
     * the root group input ports and output ports, and their access controls.
     *
     */
    public FlowInfo parse(final File flowConfigurationFile) {
        if (flowConfigurationFile == null) {
            logger.debug("Flow Configuration file was null");
            return null;
        }

        // if the flow doesn't exist or is 0 bytes, then return null
        final Path flowPath = flowConfigurationFile.toPath();
        try {
            if (!Files.exists(flowPath) || Files.size(flowPath) == 0) {
                logger.warn("Flow Configuration does not exist or was empty");
                return null;
            }
        } catch (IOException e) {
            logger.error("An error occurred determining the size of the Flow Configuration file");
            return null;
        }

        // otherwise create the appropriate input streams to read the file
        try (final InputStream in = Files.newInputStream(flowPath, StandardOpenOption.READ);
             final InputStream gzipIn = new GZIPInputStream(in)) {

            final byte[] flowBytes = IOUtils.toByteArray(gzipIn);
            if (flowBytes == null || flowBytes.length == 0) {
                logger.warn("Could not extract root group id because Flow Configuration File was empty");
                return null;
            }

            return parseJson(flowBytes);
        } catch (final IOException ex) {
            logger.error("Unable to parse flow {}", flowPath.toAbsolutePath(), ex);
            return null;
        }
    }

    private FlowInfo parseJson(final byte[] flowBytes) {
        final StandardDataFlow standardDataFlow = new StandardDataFlow(flowBytes, new byte[0], null, Collections.emptySet());
        final VersionedDataflow dataflow = standardDataFlow.getVersionedDataflow();
        final VersionedProcessGroup rootGroup = dataflow.getRootGroup();

        if (rootGroup == null) {
            return null;
        }

        final List<PortDTO> ports = new ArrayList<>();
        mapPorts(rootGroup.getInputPorts(), ports);
        mapPorts(rootGroup.getOutputPorts(), ports);

        return new FlowInfo(rootGroup.getInstanceIdentifier(), ports);
    }

    private void mapPorts(final Set<VersionedPort> versionedPorts, final List<PortDTO> portDtos) {
        if (versionedPorts == null || versionedPorts.isEmpty()) {
            return;
        }

        for (final VersionedPort port : versionedPorts) {
            final PortDTO portDto = mapPort(port);
            portDtos.add(portDto);
        }
    }

    private PortDTO mapPort(final VersionedPort port) {
        final PortDTO dto = new PortDTO();
        dto.setAllowRemoteAccess(port.isAllowRemoteAccess());
        dto.setComments(port.getComments());
        dto.setConcurrentlySchedulableTaskCount(port.getConcurrentlySchedulableTaskCount());
        dto.setId(port.getIdentifier());
        dto.setName(port.getName());
        dto.setParentGroupId(port.getGroupIdentifier());
        dto.setPosition(new PositionDTO(port.getPosition().getX(), port.getPosition().getY()));
        dto.setState(port.getScheduledState().name());
        dto.setType(port.getType().name());
        return dto;
    }
}
