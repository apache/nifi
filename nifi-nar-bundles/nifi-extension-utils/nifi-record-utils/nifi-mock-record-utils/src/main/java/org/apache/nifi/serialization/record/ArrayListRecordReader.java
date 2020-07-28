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
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ArrayListRecordReader extends AbstractControllerService implements RecordReaderFactory {
    private final List<Record> records = new ArrayList<>();
    private final RecordSchema schema;

    public ArrayListRecordReader(final RecordSchema schema) {
        this.schema = schema;
    }

    @Override
    public ArrayListReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger) {
        return new ArrayListReader(records, schema, in);
    }

    public void addRecord(final Record record) {
        this.records.add(record);
    }

    public static class ArrayListReader implements RecordReader {
        private final RecordSchema schema;
        private final Iterator<Record> itr;
        private final InputStream in;

        public ArrayListReader(final List<Record> records, final RecordSchema schema, InputStream in) {
            this.itr = records.iterator();
            this.schema = schema;
            this.in = in;
        }

        @Override
        public Record nextRecord(final boolean coerceTypes, final boolean dropUnknownFields) {
            return itr.hasNext() ? itr.next() : null;
        }

        @Override
        public RecordSchema getSchema() {
            return schema;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }
}
