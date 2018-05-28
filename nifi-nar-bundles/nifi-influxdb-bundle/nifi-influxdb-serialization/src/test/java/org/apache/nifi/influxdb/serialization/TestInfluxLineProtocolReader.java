/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.influxdb.serialization;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processors.standard.ConvertRecord;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class TestInfluxLineProtocolReader extends AbstractTestInfluxLineProtocolReader {

    @Test
    public void createReaderFromMap() throws SchemaNotFoundException, MalformedRecordException, IOException {

        RecordReader reader = readerFactory.createRecordReader(variables, null, logger);

        Assert.assertNotNull(reader);
    }

    @Test
    public void createReaderFromFlowFile() throws SchemaNotFoundException, MalformedRecordException, IOException {

        FlowFile flowFile = Mockito.mock(FlowFile.class);
        RecordReader reader = readerFactory.createRecordReader(flowFile, null, logger);

        Assert.assertNotNull(reader);
    }

    @Test
    public void schemaNotNull() throws SchemaNotFoundException, MalformedRecordException, IOException {

        FlowFile flowFile = Mockito.mock(FlowFile.class);
        RecordReader reader = readerFactory.createRecordReader(flowFile, null, logger);

        RecordSchema schema = reader.getSchema();

        Assert.assertNotNull(schema);
    }

    @Test
    public void processIncomingLineProtocolToAVRO() throws InitializationException, IOException {

        String data = "weather,location=us-midwest "
                + "field-float=82.5,field-integer=85i,field-bool=True,field-string=\"hello\" 1465839830100400200";

        ConvertRecord processor = new ConvertRecord();
        TestRunner convertRunner = TestRunners.newTestRunner(processor);

        InfluxLineProtocolReader readerFactory = new InfluxLineProtocolReader();

        convertRunner.addControllerService("record-reader", readerFactory);
        convertRunner.setProperty(readerFactory, InfluxLineProtocolReader.CHARSET, StandardCharsets.UTF_8.name());
        convertRunner.enableControllerService(readerFactory);

        AvroRecordSetWriter writerFactory = new AvroRecordSetWriter();
        convertRunner.addControllerService("record-writer", writerFactory);
        convertRunner.enableControllerService(writerFactory);

        convertRunner.setProperty("record-reader", "record-reader");
        convertRunner.setProperty("record-writer", "record-writer");

        convertRunner.enqueue(data);
        convertRunner.run();

        convertRunner.assertTransferCount("success", 1);

        MockFlowFile success = convertRunner.getFlowFilesForRelationship("success").get(0);

        DataFileStream<GenericData.Record> avroReader = new DataFileStream<>(
                new ByteArrayInputStream(success.toByteArray()), new GenericDatumReader<>());

        Assert.assertTrue(avroReader.hasNext());

        GenericData.Record next = avroReader.next();

        // measurement
        Assert.assertEquals(new Utf8("weather"), next.get("measurement"));

        // tags
        Assert.assertEquals(1, ((Map) next.get("tags")).size());
        Assert.assertEquals(new Utf8("us-midwest"), ((Map) next.get("tags")).get(new Utf8("location")));

        // fields
        Assert.assertEquals(4, ((Map) next.get("fields")).size());
        Assert.assertEquals(true, ((Map) next.get("fields")).get(new Utf8("field-bool")));
        Assert.assertEquals(new Utf8("hello"), ((Map) next.get("fields")).get(new Utf8("field-string")));
        Assert.assertEquals(85.F, ((Map) next.get("fields")).get(new Utf8("field-integer")));
        Assert.assertEquals(82.5F, ((Map) next.get("fields")).get(new Utf8("field-float")));

        // timestamp
        Assert.assertEquals(1465839830100400200L, next.get("timestamp"));

        Assert.assertFalse(avroReader.hasNext());
    }

    @Test
    public void convertMultipleRecord() throws InitializationException {

        String data = "weather,location=us-midwest temperature=82 1465839830100400200" + System.lineSeparator()
                + "weather,location=us-midwest temperature=85 1465839830100400300";

        ConvertRecord processor = new ConvertRecord();
        TestRunner convertRunner = TestRunners.newTestRunner(processor);

        InfluxLineProtocolReader readerFactory = new InfluxLineProtocolReader();

        convertRunner.addControllerService("record-reader", readerFactory);
        convertRunner.setProperty(readerFactory, InfluxLineProtocolReader.CHARSET, StandardCharsets.UTF_8.name());
        convertRunner.enableControllerService(readerFactory);

        MockRecordWriter writerFactory = new MockRecordWriter();
        convertRunner.addControllerService("record-writer", writerFactory);
        convertRunner.enableControllerService(writerFactory);

        convertRunner.setProperty("record-reader", "record-reader");
        convertRunner.setProperty("record-writer", "record-writer");

        convertRunner.enqueue(data);
        convertRunner.run();

        convertRunner.assertTransferCount("success", 1);

        MockFlowFile success = convertRunner.getFlowFilesForRelationship("success").get(0);
        Assert.assertEquals(String.valueOf(2), success.getAttribute("record.count"));
    }

    @Test
    public void convertMultipleRecordOneFail() throws InitializationException {

        String data = "weather,location=us-midwest temperature=82 1465839830100400200" + System.lineSeparator()
                + "weather,location=us-midwest 1465839830100400200";

        ConvertRecord processor = new ConvertRecord();
        TestRunner convertRunner = TestRunners.newTestRunner(processor);

        InfluxLineProtocolReader readerFactory = new InfluxLineProtocolReader();

        convertRunner.addControllerService("record-reader", readerFactory);
        convertRunner.setProperty(readerFactory, InfluxLineProtocolReader.CHARSET, StandardCharsets.UTF_8.name());
        convertRunner.enableControllerService(readerFactory);

        MockRecordWriter writerFactory = new MockRecordWriter();
        convertRunner.addControllerService("record-writer", writerFactory);
        convertRunner.enableControllerService(writerFactory);

        convertRunner.setProperty("record-reader", "record-reader");
        convertRunner.setProperty("record-writer", "record-writer");

        convertRunner.enqueue(data);
        convertRunner.run();

        convertRunner.assertTransferCount("failure", 1);

        MockFlowFile success = convertRunner.getFlowFilesForRelationship("failure").get(0);
        Assert.assertNull(success.getAttribute("record.count"));
    }
}
