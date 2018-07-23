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
package org.apache.nifi.atlas.provenance.analyzer;

import org.apache.atlas.typesystem.Referenceable;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.nifi.atlas.AtlasUtils.toQualifiedName;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_INPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_OUTPUT_PORT;

/**
 * Analyze a provenance event as a NiFi RootGroupPort for Site-to-Site communication at the server side.
 * <li>qualifiedName=rootPortGUID (example: 35dbc0ab-015e-1000-144c-a8d71255027d)
 * <li>name=portName (example: input)
 */
public class NiFiRootGroupPort extends NiFiS2S {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRootGroupPort.class);

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {

        if (!ProvenanceEventType.SEND.equals(event.getEventType())
                && !ProvenanceEventType.RECEIVE.equals(event.getEventType())) {
            return null;
        }

        final boolean isInputPort = event.getComponentType().equals("Input Port");
        final String type = isInputPort ? TYPE_NIFI_INPUT_PORT : TYPE_NIFI_OUTPUT_PORT;
        final String rootPortId = event.getComponentId();

        final S2SPort s2SPort = analyzeS2SPort(event, context.getClusterResolver());

        // Find connections connecting to/from the remote port.
        final List<ConnectionStatus> connections = isInputPort
                ? context.findConnectionFrom(rootPortId)
                : context.findConnectionTo(rootPortId);
        if (connections == null || connections.isEmpty()) {
            logger.warn("Connection was not found: {}", new Object[]{event});
            return null;
        }

        // The name of the port can be retrieved from any connection, use the first one.
        final ConnectionStatus connection = connections.get(0);
        final Referenceable ref = new Referenceable(type);
        ref.set(ATTR_NAME, isInputPort ? connection.getSourceName() : connection.getDestinationName());
        ref.set(ATTR_QUALIFIED_NAME, toQualifiedName(s2SPort.clusterName, rootPortId));

        return singleDataSetRef(event.getComponentId(), event.getEventType(), ref);
    }

    @Override
    public String targetComponentTypePattern() {
        return "^(In|Out)put Port$";
    }

    @Override
    protected String getRawProtocolPortId(ProvenanceEventRecord event) {
        return event.getComponentId();
    }
}
