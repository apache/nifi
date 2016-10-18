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
package org.apache.nifi.websocket;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.controller.ConfigurationContext;

import java.io.IOException;

/**
 * Control a WebSocket client instance.
 */
@CapabilityDescription("Control a WebSocket client instance," +
        " so that NiFi can connect with external systems via WebSocket protocol.")
public interface WebSocketClientService extends WebSocketService {

    void startClient(final ConfigurationContext context) throws Exception;

    void stopClient() throws Exception;

    void connect(final String clientId) throws IOException;

    String getTargetUri();

}
