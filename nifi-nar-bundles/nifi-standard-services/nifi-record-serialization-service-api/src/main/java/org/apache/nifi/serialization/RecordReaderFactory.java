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

package org.apache.nifi.serialization;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

/**
 * <p>
 * A Controller Service that is responsible for creating a {@link RecordReader}.
 * </p>
 */
public interface RecordReaderFactory extends ControllerService {

    /**
     * Create a RecordReader instance to read records from specified InputStream.
     * This method calls {@link #createRecordReader(Map, InputStream, ComponentLog)} with Attributes of the specified FlowFile.
     * @param flowFile Attributes of this FlowFile are used to resolve Record Schema via Expression Language dynamically. This can be null.
     * @param in InputStream containing Records. This can be null or empty stream.
     * @param logger A logger bind to a component
     * @return Created RecordReader instance
     */
    default RecordReader createRecordReader(FlowFile flowFile, InputStream in, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        return createRecordReader(flowFile == null ? Collections.emptyMap() : flowFile.getAttributes(), in, logger);
    }

    /**
     * Create a RecordReader instance to read records from specified InputStream.
     * @param variables A map contains variables which is used to resolve Record Schema via Expression Language dynamically.
     *                 This can be null or empty.
     * @param in InputStream containing Records. This can be null or empty stream.
     * @param logger A logger bind to a component
     * @return Created RecordReader instance
     */
    RecordReader createRecordReader(Map<String, String> variables, InputStream in, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException;

}
