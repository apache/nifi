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
package org.apache.nifi.cluster.protocol;

import org.apache.nifi.cluster.protocol.message.ProtocolMessage;

/**
 * A handler for processing protocol messages.
 *
 */
public interface ProtocolHandler {

    /**
     * Handles the given protocol message or throws an exception if it cannot
     * handle the message. If no response is needed by the protocol, then null
     * should be returned.
     *
     * @param msg a message
     * @return a response or null, if no response is necessary
     *
     * @throws ProtocolException if the message could not be processed
     */
    ProtocolMessage handle(ProtocolMessage msg) throws ProtocolException;

    /**
     * @param msg a message
     * @return true if the handler can process the given message; false
     * otherwise
     */
    boolean canHandle(ProtocolMessage msg);
}
