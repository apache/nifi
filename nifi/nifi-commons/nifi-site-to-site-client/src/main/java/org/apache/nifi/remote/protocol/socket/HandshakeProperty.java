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
package org.apache.nifi.remote.protocol.socket;


/**
 * Enumeration of Properties that can be used for the Site-to-Site Socket Protocol.
 */
public enum HandshakeProperty {
    /**
     * Boolean value indicating whether or not the contents of a FlowFile should be
     * GZipped when transferred.
     */
    GZIP,
    
    /**
     * The unique identifier of the port to communicate with
     */
    PORT_IDENTIFIER,
    
    /**
     * Indicates the number of milliseconds after the request was made that the client
     * will wait for a response. If no response has been received by the time this value
     * expires, the server can move on without attempting to service the request because
     * the client will have already disconnected.
     */
    REQUEST_EXPIRATION_MILLIS,
    
    /**
     * The preferred number of FlowFiles that the server should send to the client
     * when pulling data. This property was introduced in version 5 of the protocol.
     */
    BATCH_COUNT,
    
    /**
     * The preferred number of bytes that the server should send to the client when
     * pulling data. This property was introduced in version 5 of the protocol.
     */
    BATCH_SIZE,
    
    /**
     * The preferred amount of time that the server should send data to the client
     * when pulling data. This property was introduced in version 5 of the protocol.
     * Value is in milliseconds.
     */
    BATCH_DURATION;
}
