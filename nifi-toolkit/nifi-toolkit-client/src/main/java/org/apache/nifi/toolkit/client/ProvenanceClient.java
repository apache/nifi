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
package org.apache.nifi.toolkit.client;

import org.apache.nifi.web.api.entity.LatestProvenanceEventsEntity;
import org.apache.nifi.web.api.entity.LineageEntity;
import org.apache.nifi.web.api.entity.ProvenanceEntity;
import org.apache.nifi.web.api.entity.ReplayLastEventResponseEntity;

import java.io.IOException;
import java.io.InputStream;

public interface ProvenanceClient {
    ProvenanceEntity submitProvenanceQuery(ProvenanceEntity provenanceQuery) throws NiFiClientException, IOException;

    ProvenanceEntity getProvenanceQuery(String queryId) throws NiFiClientException, IOException;

    ProvenanceEntity deleteProvenanceQuery(String queryId) throws NiFiClientException, IOException;

    LineageEntity submitLineageRequest(LineageEntity lineageEntity) throws NiFiClientException, IOException;

    LineageEntity getLineageRequest(String lineageRequestId) throws NiFiClientException, IOException;

    LineageEntity deleteLineageRequest(String lineageRequestId) throws NiFiClientException, IOException;

    ReplayLastEventResponseEntity replayLastEvent(String processorId, ReplayEventNodes replayEventNodes) throws NiFiClientException, IOException;

    LatestProvenanceEventsEntity getLatestEvents(String processorId) throws NiFiClientException, IOException;

    InputStream getInputFlowFileContent(String provenanceEventId, String nodeId) throws NiFiClientException, IOException;

    InputStream getOutputFlowFileContent(String provenanceEventId, String nodeId) throws NiFiClientException, IOException;

    enum ReplayEventNodes {
        PRIMARY,
        ALL;
    }
}
