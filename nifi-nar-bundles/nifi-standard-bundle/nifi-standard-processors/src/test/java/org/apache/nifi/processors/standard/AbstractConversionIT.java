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
package org.apache.nifi.processors.standard;

import org.apache.nifi.avro.AvroReader;
import org.apache.nifi.avro.AvroReaderWithEmbeddedSchema;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVRecordSetWriter;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.xml.XMLReader;
import org.apache.nifi.xml.XMLRecordSetWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public abstract class AbstractConversionIT {
    protected RecordReaderFactory reader;
    protected Consumer<TestRunner> inputHandler;
    protected Consumer<TestRunner> readerConfigurer;

    protected RecordSetWriterFactory writer;
    protected Consumer<MockFlowFile> resultHandler;
    protected Consumer<TestRunner> writerConfigurer;

    @Before
    public void setUp() throws Exception {
        reader = null;
        inputHandler = null;
        readerConfigurer = null;

        writer = null;
        resultHandler = null;
        writerConfigurer = null;
    }

    @Test
    public void testCsvToJson() throws Exception {
        fromCsv(csvPostfix());
        toJson(jsonPostfix());

        testConversion(reader, readerConfigurer, writer, writerConfigurer, inputHandler, resultHandler);
    }

    @Test
    public void testCsvToAvro() throws Exception {
        fromCsv(csvPostfix());
        toAvro(avroPostfix());

        testConversion(reader, readerConfigurer, writer, writerConfigurer, inputHandler, resultHandler);
    }

    @Test
    public void testCsvToAvroToCsv() throws Exception {
        fromCsv(csvPostfix());

        AvroRecordSetWriter writer2 = new AvroRecordSetWriter();
        AvroReader reader2 = new AvroReader();

        toCsv(csvPostfix());

        testChain(writer2, reader2);
    }

    @Test
    public void testCsvToXml() throws Exception {
        fromCsv(csvPostfix());
        toXml(xmlPostfix());

        testConversion(reader, readerConfigurer, writer, writerConfigurer, inputHandler, resultHandler);
    }

    @Test
    public void testJsonToCsv() throws Exception {
        fromJson(jsonPostfix());
        toCsv(csvPostfix());

        testConversion(reader, readerConfigurer, writer, writerConfigurer, inputHandler, resultHandler);
    }

    @Test
    public void testJsonToAvro() throws Exception {
        fromJson(jsonPostfix());
        toAvro(avroPostfix());

        testConversion(reader, readerConfigurer, writer, writerConfigurer, inputHandler, resultHandler);
    }

    @Test
    public void testJsonToAvroToJson() throws Exception {
        fromJson(jsonPostfix());

        AvroRecordSetWriter writer2 = new AvroRecordSetWriter();
        AvroReader reader2 = new AvroReader();

        toJson(jsonPostfix());

        testChain(writer2, reader2);
    }

    @Test
    public void testAvroToCsv() throws Exception {
        fromAvro(avroPostfix());
        toCsv(csvPostfix());

        testConversion(reader, readerConfigurer, writer, writerConfigurer, inputHandler, resultHandler);
    }

    @Test
    public void testAvroToJson() throws Exception {
        fromAvro(avroPostfix());
        toJson(jsonPostfix());

        testConversion(reader, readerConfigurer, writer, writerConfigurer, inputHandler, resultHandler);
    }

    @Test
    public void testAvroToXml() throws Exception {
        fromAvro(avroPostfix());
        toXml(xmlPostfix());

        testConversion(reader, readerConfigurer, writer, writerConfigurer, inputHandler, resultHandler);
    }

    @Test
    public void testXmlToCsv() throws Exception {
        fromXml(xmlPostfix());
        toCsv(csvPostfix());

        testConversion(reader, readerConfigurer, writer, writerConfigurer, inputHandler, resultHandler);
    }

    @Test
    public void testXmlToJson() throws Exception {
        fromXml(xmlPostfix());
        toJson(jsonPostfix());

        testConversion(reader, readerConfigurer, writer, writerConfigurer, inputHandler, resultHandler);
    }

    @Test
    public void testXmlToAvro() throws Exception {
        fromXml(xmlPostfix());
        toAvro(avroPostfix());

        testConversion(reader, readerConfigurer, writer, writerConfigurer, inputHandler, resultHandler);
    }

    @Test
    public void testXmlToAvroToXml() throws Exception {
        fromXml(xmlPostfix());

        AvroRecordSetWriter writer2 = new AvroRecordSetWriter();
        AvroReader reader2 = new AvroReader();

        toXml(xmlPostfix());

        testChain(writer2, reader2);
    }

    abstract protected String csvPostfix();

    abstract protected String jsonPostfix();

    abstract protected String avroPostfix();

    abstract protected String xmlPostfix();

    protected void commonReaderConfiguration(TestRunner testRunner) {
    }

    protected void commonWriterConfiguration(TestRunner testRunner) {
    }

    protected void fromCsv(String postfix) {
        reader = new CSVReader();
        inputHandler = stringInputHandler(getContent(postfix));

        readerConfigurer = testRunner -> {
            commonReaderConfiguration(testRunner);
        };
    }

    protected void fromJson(String postfix) {
        reader = new JsonTreeReader();
        inputHandler = stringInputHandler(getContent(postfix));

        readerConfigurer = testRunner -> {
            commonReaderConfiguration(testRunner);
        };
    }

    protected void fromXml(String postfix) {
        reader = new XMLReader();
        inputHandler = stringInputHandler(getContent(postfix));

        readerConfigurer = testRunner -> {
            commonReaderConfiguration(testRunner);
            testRunner.setProperty(reader, XMLReader.RECORD_FORMAT, XMLReader.RECORD_ARRAY);
        };
    }

    protected void fromAvro(String postfix) {
        reader = new AvroReader();
        inputHandler = byteInputHandler(getByteContent(postfix));

        readerConfigurer = testRunner -> {
            commonReaderConfiguration(testRunner);
        };
    }

    protected void toCsv(String postfix) {
        writer = new CSVRecordSetWriter();
        resultHandler = stringOutputHandler(getContent(postfix));

        writerConfigurer = testRunner -> {
            commonWriterConfiguration(testRunner);
        };
    }

    protected void toJson(String postfix) {
        writer = new JsonRecordSetWriter();
        resultHandler = stringOutputHandler(getContent(postfix));

        writerConfigurer = testRunner -> {
            commonWriterConfiguration(testRunner);
            testRunner.setProperty(writer, "Pretty Print JSON", "true");
        };
    }

    protected void toXml(String postfix) {
        writer = new XMLRecordSetWriter();
        resultHandler = stringOutputHandler(getContent(postfix));

        writerConfigurer = testRunner -> {
            commonWriterConfiguration(testRunner);
            testRunner.setProperty(writer, "pretty_print_xml", "true");
            testRunner.setProperty(writer, "root_tag_name", "root");
            testRunner.setProperty(writer, "record_tag_name", "nifiRecord");
        };
    }

    protected void toAvro(String postfix) {
        writer = new AvroRecordSetWriter();
        resultHandler = mockFlowFile -> {
            try {
                List<Map<String, Object>> expected = getRecords(getByteContent(postfix));
                List<Map<String, Object>> actual = getRecords(mockFlowFile.toByteArray());

                assertEquals(expected, actual);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        writerConfigurer = testRunner -> {
            commonWriterConfiguration(testRunner);
        };
    }

    protected Consumer<TestRunner> stringInputHandler(String input) {
        return testRunner -> testRunner.enqueue(input);
    }

    protected Consumer<TestRunner> byteInputHandler(byte[] input) {
        return testRunner -> testRunner.enqueue(input);
    }

    protected Consumer<MockFlowFile> stringOutputHandler(String expected) {
        return mockFlowFile -> mockFlowFile.assertContentEquals(expected);
    }

    protected String getContent(String postfix) {
        return new String(getByteContent(postfix));
    }

    protected byte[] getByteContent(String postfix) {
        try {
            return Files.readAllBytes(Paths.get("src/test/resources/TestConversions/data.int_float_string." + postfix));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected List<Map<String, Object>> getRecords(byte[] avroData) throws IOException, MalformedRecordException {
        try (RecordReader reader = new AvroReaderWithEmbeddedSchema(new ByteArrayInputStream(avroData));) {
            return getRecords(reader);
        }
    }

    protected List<Map<String, Object>> getRecords(RecordReader reader) throws IOException, MalformedRecordException {
        List<Map<String, Object>> records = new ArrayList<>();

        Record record;
        while ((record = reader.nextRecord()) != null) {
            records.add(record.toMap());
        }

        return records;
    }

    protected void testChain(RecordSetWriterFactory writer2, RecordReaderFactory reader2) throws InitializationException {
        testConversion(reader, readerConfigurer, writer2, null,
                inputHandler,
                mockFlowFile -> {
                    try {
                        testConversion(reader2, null, writer, writerConfigurer,
                                testRunner -> testRunner.enqueue(mockFlowFile),
                                resultHandler
                        );
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    protected <R extends RecordReaderFactory, W extends RecordSetWriterFactory> void testConversion(
            R reader,
            Consumer<TestRunner> readerConfigurer,
            W writer,
            Consumer<TestRunner> writerConfigurer,
            Consumer<TestRunner> inputHandler,
            Consumer<MockFlowFile> resultHandler
    ) throws InitializationException {
        final TestRunner runner = TestRunners.newTestRunner(ConvertRecord.class);

        String readerId = UUID.randomUUID().toString();
        String writerId = UUID.randomUUID().toString();

        runner.addControllerService(readerId, reader);
        runner.addControllerService(writerId, writer);

        Optional.ofNullable(readerConfigurer).ifPresent(_configurer -> _configurer.accept(runner));
        Optional.ofNullable(writerConfigurer).ifPresent(_configurer -> _configurer.accept(runner));

        runner.enableControllerService(reader);
        runner.enableControllerService(writer);

        runner.setProperty(ConvertRecord.RECORD_READER, readerId);
        runner.setProperty(ConvertRecord.RECORD_WRITER, writerId);

        inputHandler.accept(runner);

        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertRecord.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(ConvertRecord.REL_SUCCESS).get(0);

        resultHandler.accept(flowFile);
    }
}
