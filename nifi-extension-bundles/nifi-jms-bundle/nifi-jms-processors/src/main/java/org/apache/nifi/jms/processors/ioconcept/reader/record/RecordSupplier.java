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
package org.apache.nifi.jms.processors.ioconcept.reader.record;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.jms.processors.ioconcept.reader.MessageHandler;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

public class RecordSupplier {

    private final RecordReaderFactory readerFactory;
    private final RecordSetWriterFactory writerFactory;

    public RecordSupplier(RecordReaderFactory readerFactory, RecordSetWriterFactory writerFactory) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
    }

    public void process(FlowFile flowfile, InputStream in, AtomicInteger processedRecords, Long processFromIndex, ComponentLog logger, MessageHandler messageHandler) throws IOException {

        try (final RecordReader reader = readerFactory.createRecordReader(flowfile, in, logger)) {
            final RecordSet recordSet = reader.createRecordSet();

            final RecordSchema schema = writerFactory.getSchema(flowfile.getAttributes(), recordSet.getSchema());

            final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);

            Record record;
            while ((record = recordSet.next()) != null) {
                if (processFromIndex != null && processedRecords.get() < processFromIndex) {
                    processedRecords.getAndIncrement();
                    continue;
                }

                baos.reset();

                try (final RecordSetWriter writer = writerFactory.createWriter(logger, schema, baos, flowfile)) {
                    writer.write(record);
                    writer.flush();
                }

                final byte[] messageContent = baos.toByteArray();

                messageHandler.handle(messageContent);

                processedRecords.getAndIncrement();
            }
        } catch (SchemaNotFoundException | MalformedRecordException e) {
            throw new ProcessException("An error happened during creating components for serialization.", e);
        }
    }

}
