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
package org.apache.nifi.tests.system;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.flow.VersionedConnector;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * Reads connector sections of a stopped node's conf/flow.json.gz.
 */
public final class FlowDefinitionFiles {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private FlowDefinitionFiles() {
    }

    public static VersionedConnector readConnector(final NiFiInstance node, final String connectorId) throws IOException {
        final VersionedDataflow dataflow = read(flowFile(node));
        return find(dataflow, connectorId);
    }

    private static VersionedConnector find(final VersionedDataflow dataflow, final String connectorId) {
        if (dataflow.getConnectors() == null) {
            throw new IllegalStateException("flow.json.gz has no connectors");
        }
        for (final VersionedConnector connector : dataflow.getConnectors()) {
            if (connectorId.equals(connector.getInstanceIdentifier())) {
                return connector;
            }
        }
        throw new IllegalStateException("flow.json.gz has no connector " + connectorId);
    }

    private static File flowFile(final NiFiInstance node) {
        return new File(new File(node.getInstanceDirectory(), "conf"), "flow.json.gz");
    }

    private static VersionedDataflow read(final File flowFile) throws IOException {
        try (InputStream fileIn = new FileInputStream(flowFile);
             InputStream gzipIn = new GZIPInputStream(fileIn)) {
            return OBJECT_MAPPER.readValue(gzipIn, VersionedDataflow.class);
        }
    }
}
