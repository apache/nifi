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
package org.apache.nifi.jasn1.util;

import com.beanit.asn1bean.ber.types.BerType;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.jasn1.JASN1Reader;
import org.apache.nifi.jasn1.JASN1RecordReader;
import org.apache.nifi.jasn1.RecordSchemaProvider;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockComponentLog;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.function.Function;

import static org.apache.nifi.jasn1.util.RecordTestUtil.assertRecordsEqual;
import static org.junit.Assert.assertEquals;

public interface JASN1ReadRecordTester {
    default void testReadRecord(String dataFile, BerType berObject, Map<String, Object> expectedValues, RecordSchema expectedSchema) throws IOException, MalformedRecordException {
        testReadRecord(dataFile, berObject, __ -> expectedValues, __ -> expectedSchema);
    }

    default void testReadRecord(
        String dataFile,
        BerType berObject,
        Function<Record,
        Map<String, Object>> expectedValuesProvider,
        Function<Record, RecordSchema> expectedSchemaProvider
    ) throws IOException, MalformedRecordException {
        JASN1DataWriter.write(berObject, dataFile);

        try (final InputStream input = new FileInputStream(dataFile)) {

            final JASN1RecordReader reader = new JASN1RecordReader(
                    berObject.getClass().getName(),
                    null,
                    new RecordSchemaProvider(),
                    Thread.currentThread().getContextClassLoader(),
                    null,
                    input,
                    new MockComponentLog("id", new JASN1Reader())
            );

            Record actual = reader.nextRecord(true, false);
            RecordSchema actualSchema = actual.getSchema();

            Record expected = new MapRecord(actualSchema, expectedValuesProvider.apply(actual));
            RecordSchema expectedSchema = expectedSchemaProvider.apply(actual);

            assertRecordsEqual(expected, actual);
            assertEquals(expectedSchema, actualSchema);
        }
    }

    default String octetStringExpectedValueConverter(byte[] rawValue) {
        return Hex.encodeHexString(rawValue).toUpperCase();
    }
}
