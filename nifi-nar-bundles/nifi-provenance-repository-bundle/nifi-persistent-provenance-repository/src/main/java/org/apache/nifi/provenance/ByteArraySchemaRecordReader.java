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

package org.apache.nifi.provenance;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.provenance.schema.EventRecord;
import org.apache.nifi.provenance.serialization.CompressableRecordReader;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.repository.schema.SchemaRecordReader;
import org.apache.nifi.stream.io.LimitingInputStream;
import org.apache.nifi.stream.io.StreamUtils;


public class ByteArraySchemaRecordReader extends CompressableRecordReader {
    private RecordSchema schema; // effectively final
    private SchemaRecordReader recordReader;  // effectively final

    public ByteArraySchemaRecordReader(final InputStream in, final String filename, final int maxAttributeChars) throws IOException {
        super(in, filename, maxAttributeChars);
    }

    public ByteArraySchemaRecordReader(final InputStream in, final String filename, final TocReader tocReader, final int maxAttributeChars) throws IOException {
        super(in, filename, tocReader, maxAttributeChars);
    }

    private void verifySerializationVersion(final int serializationVersion) {
        if (serializationVersion > ByteArraySchemaRecordWriter.SERIALIZATION_VERSION) {
            throw new IllegalArgumentException("Unable to deserialize record because the version is " + serializationVersion
                + " and supported versions are 1-" + ByteArraySchemaRecordWriter.SERIALIZATION_VERSION);
        }
    }

    @Override
    protected void readHeader(final DataInputStream in, final int serializationVersion) throws IOException {
        verifySerializationVersion(serializationVersion);
        final int schemaLength = in.readInt();
        final byte[] buffer = new byte[schemaLength];
        StreamUtils.fillBuffer(in, buffer);

        try (final ByteArrayInputStream bais = new ByteArrayInputStream(buffer)) {
            schema = RecordSchema.readFrom(bais);
        }

        recordReader = SchemaRecordReader.fromSchema(schema);
    }

    @Override
    protected StandardProvenanceEventRecord nextRecord(final DataInputStream in, final int serializationVersion) throws IOException {
        verifySerializationVersion(serializationVersion);
        final long byteOffset = getBytesConsumed();
        final int recordLength = in.readInt();

        final InputStream limitedIn = new LimitingInputStream(in, recordLength);
        final Record eventRecord = recordReader.readRecord(limitedIn);
        if (eventRecord == null) {
            return null;
        }

        return EventRecord.getEvent(eventRecord, getFilename(), byteOffset, getMaxAttributeLength());
    }

}
