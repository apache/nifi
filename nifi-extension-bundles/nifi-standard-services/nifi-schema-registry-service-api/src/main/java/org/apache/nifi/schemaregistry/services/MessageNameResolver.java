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
package org.apache.nifi.schemaregistry.services;

import org.apache.nifi.controller.ControllerService;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
/**
 * An interface for resolving message names from schema definitions and input streams.
 * This interface is typically used in scenarios where message types need to be determined
 * dynamically from the content of the message and the associated schema definition.
 * <p>
 * Implementations of this interface can be used to extract or derive message names from
 * various sources such as message headers, content, or schema metadata, enabling proper
 * message processing and routing based on the resolved message type.
 * </p>
 */
public interface MessageNameResolver extends ControllerService {

    /**
     * Resolves and returns the message name based on the provided schema definition and input stream.
     * <p>
     * This method analyzes the given schema definition and input stream to determine the appropriate
     * message name. The resolution strategy depends on the specific implementation and may involve
     * parsing message headers, analyzing message content, or consulting schema metadata.
     * </p>
     *
     * @param variables additional variables that may influence the resolution process, such as context-specific information
     * @param schemaDefinition the schema definition containing schema information and metadata
     * @param inputStream      the input stream containing the message data to analyze
     * @return the resolved message name encapsulating the determined message type
     * @throws IOException if an I/O error occurs while reading from the input stream or processing the schema
     */
    MessageName getMessageName(final Map<String, String> variables, final SchemaDefinition schemaDefinition, final InputStream inputStream) throws IOException;
}
