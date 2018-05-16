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

import org.apache.nifi.processor.Processor;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WebSocketMessageRouters {
    private Map<String, WebSocketMessageRouter> routers = new ConcurrentHashMap<>();

    private synchronized WebSocketMessageRouter getRouterOrCreate(final String endpointId) {
        WebSocketMessageRouter router = routers.get(endpointId);
        if (router == null) {
            router = new WebSocketMessageRouter(endpointId);
            routers.put(endpointId, router);
        }
        return router;
    }

    public WebSocketMessageRouter getRouterOrFail(final String endpointId) throws WebSocketConfigurationException {
        final WebSocketMessageRouter router = routers.get(endpointId);

        if (router == null) {
            throw new WebSocketConfigurationException("No WebSocket router is bound with endpointId: " + endpointId);
        }
        return router;
    }

    public synchronized void registerProcessor(final String endpointId, final Processor processor) throws WebSocketConfigurationException {
        final WebSocketMessageRouter router = getRouterOrCreate(endpointId);
        router.registerProcessor(processor);
    }

    public boolean isProcessorRegistered(final String endpointId, final Processor processor) {
        try {
            final WebSocketMessageRouter router = getRouterOrFail(endpointId);
            return router.isProcessorRegistered(processor);
        } catch (WebSocketConfigurationException e) {
            return false;
        }
    }

    public synchronized void deregisterProcessor(final String endpointId, final Processor processor) throws WebSocketConfigurationException {
        final WebSocketMessageRouter router = getRouterOrFail(endpointId);
        routers.remove(endpointId);
        router.deregisterProcessor(processor);
    }

    public void sendMessage(final String endpointId, final String sessionId, final SendMessage sendMessage) throws IOException, WebSocketConfigurationException {
        final WebSocketMessageRouter router = getRouterOrFail(endpointId);
        router.sendMessage(sessionId, sendMessage);
    }

}
