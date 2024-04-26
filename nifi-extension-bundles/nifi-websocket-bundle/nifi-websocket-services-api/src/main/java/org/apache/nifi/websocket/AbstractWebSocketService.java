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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.Processor;

import java.io.IOException;

public abstract class AbstractWebSocketService extends AbstractControllerService implements WebSocketService {

    final protected WebSocketMessageRouters routers = new WebSocketMessageRouters();

    @Override
    public void registerProcessor(final String endpointId, final Processor processor) throws WebSocketConfigurationException {
        routers.registerProcessor(endpointId, processor);
    }

    @Override
    public boolean isProcessorRegistered(final String endpointId, final Processor processor) {
        return routers.isProcessorRegistered(endpointId, processor);
    }

    @Override
    public void deregisterProcessor(final String endpointId, final Processor processor) throws WebSocketConfigurationException {
        routers.deregisterProcessor(endpointId, processor);
    }

    @Override
    public void sendMessage(final String endpointId, final String sessionId, final SendMessage sendMessage) throws IOException, WebSocketConfigurationException {
        routers.sendMessage(endpointId, sessionId, sendMessage);
    }

}
