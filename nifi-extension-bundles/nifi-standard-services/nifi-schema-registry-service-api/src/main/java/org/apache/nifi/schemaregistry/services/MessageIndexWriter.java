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
import java.io.OutputStream;
import java.util.Map;

/**
 * An interface for writing message index information that locates a specific message within
 * a schema definition containing multiple message declarations.
 * <p>
 * This is the write-side counterpart to {@link MessageNameResolver}: given a message name that
 * has already been determined, it encodes the path to that message within the schema definition
 * and writes it to an output stream, enabling a reader to later resolve the same message name
 * from the encoded path.
 * </p>
 */
public interface MessageIndexWriter extends ControllerService {

    /**
     * Writes the message index identifying the given message name within the provided schema
     * definition to the provided output stream.
     * <p>
     * This method analyzes the given schema definition to determine the path of the target
     * message and encodes that path to the output stream. The encoding strategy depends on the
     * specific implementation and may involve navigating nested message declarations or
     * consulting schema metadata.
     * </p>
     *
     * @param variables         additional variables that may influence the encoding process, such as context-specific information
     * @param schemaDefinition  the schema definition containing schema information and metadata
     * @param messageName       the name of the message whose index should be written
     * @param outputStream      the output stream to which the message index should be written
     * @throws IOException if an I/O error occurs while writing to the output stream or processing the schema
     */
    void writeMessageIndex(final Map<String, String> variables, final SchemaDefinition schemaDefinition, final MessageName messageName, final OutputStream outputStream) throws IOException;
}
