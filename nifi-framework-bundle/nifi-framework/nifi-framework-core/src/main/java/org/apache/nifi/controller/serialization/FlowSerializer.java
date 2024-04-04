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
package org.apache.nifi.controller.serialization;

import java.io.OutputStream;

import org.apache.nifi.controller.FlowController;

/**
 * Serializes the flow configuration of a controller instance to an output stream.
 *
 */
public interface FlowSerializer<T> {

    public static final String ENC_PREFIX = "enc{";
    public static final String ENC_SUFFIX = "}";

    /**
     * Transforms the flow configuration of a controller instance into something that can serialized
     *
     * @param controller a controller
     * @param stateLookup a lookup that can be used to determine the ScheduledState of a Processor
     *
     * @return a form of the flow configuration that can be serialized by the {@link #serialize(Object, OutputStream)} method
     * @throws FlowSerializationException if serialization failed
     */
    T transform(FlowController controller, ScheduledStateLookup stateLookup) throws FlowSerializationException;

    /**
     * Serializes the flow configuration to the given Output Stream
     *
     * @param flowConfiguration the flow configuration to serialize
     * @param os the output stream to serialize to
     * @throws FlowSerializationException if serialization failed
     */
    void serialize(T flowConfiguration, OutputStream os) throws FlowSerializationException;
}
