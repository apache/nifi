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
package org.apache.nifi.c2.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.api.Payload;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2HeartbeatResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class C2ClientBase implements C2Client {

    private static final Logger logger = LoggerFactory.getLogger(C2ClientBase.class);

    private final ObjectMapper objectMapper;

    public C2ClientBase() {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public C2HeartbeatResponse publishHeartbeat(C2Heartbeat heartbeat) throws IOException {
        String heartbeatString = "";

        if (heartbeat != null) {

            final Payload payload = new Payload(heartbeat);
            try {
                heartbeatString = convertPayloadToString(payload);
            } catch (IOException e) {
                logger.info("Instance is currently restarting and cannot heartbeat.");
                return null;
            }

            C2HeartbeatResponse c2HeartbeatResponse = sendHeartbeat(heartbeatString);
            return c2HeartbeatResponse;
        }
        return null;
    }

    private String convertPayloadToString(Payload payload) throws IOException {
        logger.trace("Generating payload as string...");
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        String heartbeatString = "";
        if (payload != null) {
            heartbeatString = objectMapper.writeValueAsString(payload);
            logger.trace("Payload: {}", heartbeatString);
        }
        return heartbeatString;
    }

    protected abstract C2HeartbeatResponse sendHeartbeat(final String heartbeatString);
}
