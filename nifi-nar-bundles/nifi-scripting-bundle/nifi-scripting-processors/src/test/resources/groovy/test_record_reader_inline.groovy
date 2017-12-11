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

import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.logging.ComponentLog
import org.apache.nifi.schema.access.SchemaNotFoundException
import org.apache.nifi.serialization.MalformedRecordException
import org.apache.nifi.serialization.RecordReader
import org.apache.nifi.serialization.RecordReaderFactory
import org.apache.nifi.serialization.SimpleRecordSchema
import org.apache.nifi.serialization.record.MapRecord
import org.apache.nifi.serialization.record.Record
import org.apache.nifi.serialization.record.RecordField
import org.apache.nifi.serialization.record.RecordFieldType
import org.apache.nifi.serialization.record.RecordSchema


class GroovyRecordReader implements RecordReader {

    def recordSchema = new SimpleRecordSchema(
            [new RecordField('id', RecordFieldType.INT.dataType),
             new RecordField('name', RecordFieldType.STRING.dataType),
             new RecordField('code', RecordFieldType.INT.dataType)]
    )

    def recordIterator = [
            new MapRecord(recordSchema, ['id': 1, 'name': 'John', 'code': 100]),
            new MapRecord(recordSchema, ['id': 2, 'name': 'Mary', 'code': 200]),
            new MapRecord(recordSchema, ['id': 3, 'name': 'Ramon', 'code': 300])
    ].iterator()

    Record nextRecord(boolean coerceTypes, boolean dropUnknown) throws IOException, MalformedRecordException {
        return recordIterator.hasNext() ? recordIterator.next() : null
    }

    RecordSchema getSchema() throws MalformedRecordException {
        return recordSchema
    }

    void close() throws IOException {
    }
}

class GroovyRecordReaderFactory extends AbstractControllerService implements RecordReaderFactory {

    RecordReader createRecordReader(Map<String, String> variables, InputStream inputStream, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        return new GroovyRecordReader()
    }
}

reader = new GroovyRecordReaderFactory()
