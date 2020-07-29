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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.record.Record;

public class WriteAvroResultWithSchema extends AbstractRecordSetWriter {

    private final DataFileWriter<GenericRecord> dataFileWriter;
    private final Schema schema;

    public WriteAvroResultWithSchema(final Schema schema, final OutputStream out, final CodecFactory codec) throws IOException {
        super(out);
        this.schema = schema;

        final GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.setCodec(codec);
        dataFileWriter.create(schema, out);
    }

    @Override
    public void close() throws IOException {
        dataFileWriter.close();
    }

    @Override
    public void flush() throws IOException {
        dataFileWriter.flush();
    }

    @Override
    public Map<String, String> writeRecord(final Record record) throws IOException {
        final GenericRecord rec = AvroTypeUtil.createAvroRecord(record, schema);
        dataFileWriter.append(rec);
        return Collections.emptyMap();
    }

    @Override
    public String getMimeType() {
        return "application/avro-binary";
    }
}
