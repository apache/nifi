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
package org.apache.nifi.atlas.provenance.analyzer.unknown;

import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.DataSetRefs;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;

import java.util.List;

/**
 * Analyze a CREATE event and create 'nifi_data' when there is no specific Analyzer implementation found.
 * <li>qualifiedName=NiFiComponentId@namespace (example: processor GUID@ns1)
 * <li>name=NiFiComponentType (example: GenerateFlowFile)
 */
public class Create extends UnknownInput {

    @Override
    public DataSetRefs analyze(AnalysisContext context, ProvenanceEventRecord event) {

        // Check if this component is a processor that generates data.
        final String componentId = event.getComponentId();
        final List<ConnectionStatus> incomingConnections = context.findConnectionTo(componentId);
        if (incomingConnections != null && !incomingConnections.isEmpty()) {
            return null;
        }

        return super.analyze(context, event);
    }

    @Override
    public ProvenanceEventType targetProvenanceEventType() {
        return ProvenanceEventType.CREATE;
    }
}
