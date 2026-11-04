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
package org.apache.nifi.processors.cql.mock;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;

/**
 * A {@link RecordSetWriterFactory} whose {@link #createWriter} always fails. There is no existing NiFi mock for
 * this failure mode: {@code MockRecordWriter} can only fail mid-write (via its {@code failAfterN} constructor
 * argument), not at writer-creation time. Used to exercise ExecuteCQLQueryCallback's cleanup path when the
 * output writer itself cannot be created.
 */
public class FailingRecordSetWriterFactory extends AbstractControllerService implements RecordSetWriterFactory {
    @Override
    public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) {
        return new SimpleRecordSchema(Collections.emptyList());
    }

    @Override
    public RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema, OutputStream out, Map<String, String> variables)
            throws SchemaNotFoundException, IOException {
        throw new IOException("Unit test intentionally failing to create a RecordSetWriter");
    }
}
