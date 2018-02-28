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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.nifi.logging.ComponentLog;
import org.apache.avro.Schema;
import org.apache.nifi.json.JsonTreeRowRecordReader;
import org.apache.nifi.schema.access.SchemaNameAsAttribute;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.Test;
import org.mockito.Mockito;
/**
 * Verifies the issue described at NIFI-4901 is solved.
 */
public class TestIssueOptionalBoolean {

    private static final String SCHEMA = "{" + "   \"type\":\"record\"," + "   \"name\":\"foo\"," + "   \"fields\":["
            + "      {" + "         \"name\":\"isSwap\"," + "         \"type\":[" + "            \"boolean\","
            + "            \"null\"" + "         ]" + "      }     " + "   ]" + "}";

    private static final String JSON = "{" + "  \"isSwap\": {" + "    \"boolean\": true" + "  }" + "}";

    private final String dateFormat = RecordFieldType.DATE.getDefaultFormat();
    private final String timeFormat = RecordFieldType.TIME.getDefaultFormat();
    private final String timestampFormat = RecordFieldType.TIMESTAMP.getDefaultFormat();

    @Test
    public void testIssue() throws Exception {
        final Schema avroSchema = new Schema.Parser().parse(SCHEMA);
        final RecordSchema schema = AvroTypeUtil.createSchema(avroSchema);
        try (final InputStream in = new ByteArrayInputStream(JSON.getBytes(StandardCharsets.UTF_8));
                final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class),
                        schema, dateFormat, timeFormat, timestampFormat);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                RecordSetWriter writer = new WriteAvroResultWithExternalSchema(avroSchema, schema,
                        new SchemaNameAsAttribute(), out)) {
            Record record;
            while ((record = reader.nextRecord()) != null) {
                writer.write(record);
            }
        }
    }

}
