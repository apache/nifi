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
import org.apache.nifi.processor.io.StreamCallback;
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
     *
     * @param in InputStream containing Records.
     * @param logger A logger bound to a component
     *
     * @return Created RecordReader instance
     */
    default RecordReader createRecordReader(FlowFile flowFile, InputStream in, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        return createRecordReader(flowFile == null ? Collections.emptyMap() : flowFile.getAttributes(), in, logger);
    }

    /**
     * <p>
     * Create a RecordReader instance to read records from specified InputStream.
     * <p>
     *
     * <p>
     * Many Record Readers will need to read from the Input Stream in order to ascertain the appropriate Schema, and then
     * re-read some of the data in order to read the Records. As a result, it is common for Readers to use
     * {@link InputStream#mark(int) mark}/{@link InputStream#reset() reset}, so this should be considered when providing an
     * InputStream. The {@link InputStream} that is provided by {@link org.apache.nifi.processor.ProcessSession#read(FlowFile) SessionProcess.read} /
     * {@link org.apache.nifi.processor.ProcessSession#write(FlowFile, StreamCallback) ProcessSession.write} does provide the ability to use mark/reset
     * and does so in a way that allows any number of bytes to be read before resetting without requiring that data be buffered. Therefore, it is recommended
     * that when providing an InputStream from {@link org.apache.nifi.processor.ProcessSession ProcessSession} that the InputStream not be wrapped in a
     * BufferedInputStream. However, if the stream is coming from elsewhere, it may be necessary.
     * </p>
     *
     * @param variables A map containing variables which is used to resolve the Record Schema dynamically via Expression Language.
     *                 This can be null or empty.
     * @param in InputStream containing Records.
     * @param logger A logger bound to a component
     *
     * @return Created RecordReader instance
     */
    RecordReader createRecordReader(Map<String, String> variables, InputStream in, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException;

}
