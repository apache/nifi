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

package org.apache.nifi.processors.websocket;

public final class WebSocketProcessorAttributes{

    public static final String ATTR_WS_CS_ID = "websocket.controller.service.id";
    public static final String ATTR_WS_SESSION_ID = "websocket.session.id";
    public static final String ATTR_WS_ENDPOINT_ID = "websocket.endpoint.id";
    public static final String ATTR_WS_FAILURE_DETAIL = "websocket.failure.detail";
    public static final String ATTR_WS_MESSAGE_TYPE = "websocket.message.type";
    public static final String ATTR_WS_LOCAL_ADDRESS = "websocket.local.address";
    public static final String ATTR_WS_REMOTE_ADDRESS = "websocket.remote.address";

    private WebSocketProcessorAttributes() {
    }
}
