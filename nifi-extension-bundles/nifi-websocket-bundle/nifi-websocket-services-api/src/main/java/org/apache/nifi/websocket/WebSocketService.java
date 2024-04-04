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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.ssl.RestrictedSSLContextService;

import java.io.IOException;

/**
 * Control an embedded WebSocket service instance.
 */
public interface WebSocketService extends ControllerService {

    PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service to use in order to secure the server. If specified, the server will accept only WSS requests; "
                    + "otherwise, the server will accept only WS requests")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
            .build();

    void registerProcessor(final String endpointId, final Processor processor) throws WebSocketConfigurationException;

    boolean isProcessorRegistered(final String endpointId, final Processor processor);

    void deregisterProcessor(final String endpointId, final Processor processor) throws WebSocketConfigurationException;

    void sendMessage(final String endpointId, final String sessionId, final SendMessage sendMessage) throws IOException, WebSocketConfigurationException;

}
