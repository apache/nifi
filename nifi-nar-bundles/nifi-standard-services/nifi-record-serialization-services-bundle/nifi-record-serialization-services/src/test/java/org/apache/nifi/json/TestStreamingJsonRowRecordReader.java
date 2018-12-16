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

package org.apache.nifi.json;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;

public class TestStreamingJsonRowRecordReader {
    @Test
    public void test() throws Exception {
        InputStream _schema = getClass().getResourceAsStream("/avro/streaming-record.avsc");
        String schema = IOUtils.toString(_schema, "UTF-8");
        InputStream data = getClass().getResourceAsStream("/json/streaming-records.json");
        RecordSchema recordSchema = AvroTypeUtil.createSchema(new Schema.Parser().parse(schema));

        RecordReader reader = new StreamingJsonRowRecordReader("$.messages[*]", recordSchema, data, null, null, null, null);
        int count = 0;
        while (reader.nextRecord() != null) {
            count++;
        }
        Assert.assertEquals(4, count);
    }
}
