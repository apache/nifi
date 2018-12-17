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
package org.apache.nifi.serialization.record;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An implementation that is suitable for testing that does not serialize the data to an Output Stream but insted just buffers the data into an
 * ArrayList and then provides that List of written records to the user
 */
public class ArrayListRecordWriter extends AbstractControllerService implements RecordSetWriterFactory {
    private final List<Record> records = new ArrayList<>();
    private final RecordSchema schema;

    public ArrayListRecordWriter(final RecordSchema schema) {
        this.schema = schema;
    }


    @Override
    public RecordSchema getSchema(final Map<String, String> variables, final RecordSchema readSchema) {
        return schema;
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out) {
        return new ArrayListRecordSetWriter(records);
    }

    public List<Record> getRecordsWritten() {
        return Collections.unmodifiableList(records);
    }

    public static class ArrayListRecordSetWriter implements RecordSetWriter {
        private final List<Record> records;

        public ArrayListRecordSetWriter(final List<Record> records) {
            this.records = records;
        }

        @Override
        public WriteResult write(final RecordSet recordSet) throws IOException {
            int count = 0;

            Record record;
            while ((record = recordSet.next()) != null) {
                records.add(record);
                count++;
            }

            return WriteResult.of(count, Collections.emptyMap());
        }

        @Override
        public void beginRecordSet() {
        }

        @Override
        public WriteResult finishRecordSet() {
            return null;
        }

        @Override
        public WriteResult write(final Record record) {
            records.add(record);
            return WriteResult.of(1, Collections.emptyMap());
        }

        @Override
        public String getMimeType() {
            return null;
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
