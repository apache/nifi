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

package org.apache.nifi.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;

public class AvroReaderWithEmbeddedSchema extends AvroRecordReader {
    private final DataFileStream<GenericRecord> dataFileStream;
    private final InputStream in;
    private final Schema avroSchema;
    private final RecordSchema recordSchema;

    public AvroReaderWithEmbeddedSchema(final InputStream in) throws IOException {
        this.in = in;
        dataFileStream = new DataFileStream<>(in, new NonCachingDatumReader<>());
        this.avroSchema = dataFileStream.getSchema();
        recordSchema = AvroTypeUtil.createSchema(avroSchema);
    }

    @Override
    public void close() throws IOException {
        dataFileStream.close();
        in.close();
    }

    @Override
    protected GenericRecord nextAvroRecord() {
        if (!dataFileStream.hasNext()) {
            return null;
        }

        return dataFileStream.next();
    }

    @Override
    public RecordSchema getSchema() {
        return recordSchema;
    }
}
