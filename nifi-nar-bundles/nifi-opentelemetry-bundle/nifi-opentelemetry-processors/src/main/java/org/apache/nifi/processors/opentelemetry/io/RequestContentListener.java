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
package org.apache.nifi.processors.opentelemetry.io;

import org.apache.nifi.processors.opentelemetry.protocol.ServiceRequestDescription;
import org.apache.nifi.processors.opentelemetry.protocol.ServiceResponse;

import java.nio.ByteBuffer;

/**
 * Request Content Listener abstracts deserialization processing for Export Service Requests
 */
public interface RequestContentListener {
    /**
     * On Request handles buffer deserialization and returns request status
     *
     * @param buffer Request Content Buffer to be processed
     * @param serviceRequestDescription Service Request Description describes reader attributes
     * @return Service Response indicates processing results
     */
    ServiceResponse onRequest(ByteBuffer buffer, ServiceRequestDescription serviceRequestDescription);
}
