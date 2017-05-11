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

import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;

/**
 * <p>
 * A Controller Service that is responsible for creating a {@link RecordSetWriter}. The writer is created
 * based on a FlowFile and an InputStream for that FlowFile, but it is important to note that this the FlowFile passed
 * to the {@link #createWriter(ComponentLog, FlowFile, InputStream)} may not be the FlowFile that the Writer will writer to.
 * Rather, it is the FlowFile and InputStream from which the Writer's Schema should be determined. This is done because most
 * Processors that make use of Record Writers also make use of Record Readers and the schema for the output is often determined
 * by either reading the schema from the content of the input FlowFile or from referencing attributes of the
 * input FlowFile.
 * </p>
 *
 * <p>
 * PLEASE NOTE: This interface is still considered 'unstable' and may change in a non-backward-compatible
 * manner between minor or incremental releases of NiFi.
 * </p>
 */
public interface RecordSetWriterFactory extends ControllerService {

    /**
     * <p>
     * Returns the Schema that will be used for writing Records. Note that the FlowFile and InputStream that are given
     * may well be different than the FlowFile that the writer will write to. The given FlowFile and InputStream are
     * intended to be used for determining the schema that should be used when writing records.
     * </p>
     *
     * @param flowFile the FlowFile from which the schema should be determined.
     * @param content the contents of the FlowFile from which to determine the schema
     * @return the Schema that should be used for writing Records
     * @throws SchemaNotFoundException if unable to find the schema
     */
    RecordSchema getSchema(FlowFile flowFile, InputStream content) throws SchemaNotFoundException, IOException;

    /**
     * <p>
     * Creates a new RecordSetWriter that is capable of writing record contents to an OutputStream.
     * </p>
     *
     * @param logger the logger to use when logging information. This is passed in, rather than using the logger of the Controller Service
     *            because it allows messages to be logged for the component that is calling this Controller Service.
     * @param schema the schema that will be used for writing records
     *
     * @return a RecordSetWriter that can write record sets to an OutputStream
     * @throws IOException if unable to read from the given InputStream
     */
    RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema) throws SchemaNotFoundException, IOException;
}
