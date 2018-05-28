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
package org.apache.nifi.processors.influxdb;

import avro.shaded.com.google.common.collect.Maps;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.influxdb.WriteOptions.ComplexFieldBehaviour;
import org.apache.nifi.processors.influxdb.WriteOptions.MissingItemsBehaviour;
import org.apache.nifi.processors.influxdb.WriteOptions.NullValueBehaviour;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestPutInfluxDBRecordSettings extends AbstractTestPutInfluxDBRecord {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void defaultSettingsValid() {

        testRunner.assertValid();
    }

    @Test
    public void defaultSettings() {

        InfluxDB influxDB = processor.getInfluxDB(testRunner.getProcessContext());

        // GZIP
        Mockito.verify(influxDB, Mockito.times(1)).disableGzip();
        Mockito.verify(influxDB, Mockito.times(0)).enableGzip();

        // LogLevel
        Mockito.verify(influxDB, Mockito.times(1)).setLogLevel(InfluxDB.LogLevel.NONE);

        // Consistency level
        Mockito.verify(influxDB, Mockito.times(1)).setConsistency(InfluxDB.ConsistencyLevel.ONE);

        // Batch
        Mockito.verify(influxDB, Mockito.times(1)).disableBatch();
        Mockito.verify(influxDB, Mockito.times(0)).enableBatch();
        Mockito.verify(influxDB, Mockito.times(0)).enableBatch(Mockito.any());
    }

    @Test
    public void influxDBServiceNotDefined() {

        testRunner.removeProperty(PutInfluxDBRecord.INFLUX_DB_SERVICE);

        testRunner.assertNotValid();
    }

    @Test
    public void databaseName() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value");

        testRunner.enqueue("");
        testRunner.run();

        Assert.assertEquals("nifi-database", pointCapture.getValue().getDatabase());
    }

    @Test
    public void databaseNameExpression() {

        testRunner.setProperty(PutInfluxDBRecord.DB_NAME, "${databaseProperty}");

        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("databaseProperty", "dynamic-database-name");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value");

        testRunner.enqueue("", attributes);
        testRunner.run();

        Assert.assertEquals("dynamic-database-name", pointCapture.getValue().getDatabase());
    }

    @Test
    public void retentionPolicy() {

        testRunner.setProperty(PutInfluxDBRecord.RETENTION_POLICY, "custom-retention");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value");

        testRunner.enqueue("");
        testRunner.run();

        Assert.assertEquals("custom-retention", pointCapture.getValue().getRetentionPolicy());
    }

    @Test
    public void retentionPolicyNotDefined() {

        testRunner.setProperty(PutInfluxDBRecord.RETENTION_POLICY, "${retentionNotDefinedProperty}");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value");

        testRunner.enqueue("");
        testRunner.run();

        Assert.assertEquals(WriteOptions.DEFAULT_RETENTION_POLICY, pointCapture.getValue().getRetentionPolicy());
    }

    @Test
    public void enableGZIP() {

        testRunner.setProperty(PutInfluxDBRecord.ENABLE_GZIP, "true");

        InfluxDB influxDB = processor.getInfluxDB(testRunner.getProcessContext());

        Mockito.verify(influxDB, Mockito.times(0)).disableGzip();
        Mockito.verify(influxDB, Mockito.times(1)).enableGzip();
    }

    @Test
    public void logLevel() {

        testRunner.setProperty(PutInfluxDBRecord.LOG_LEVEL, InfluxDB.LogLevel.FULL.toString());

        InfluxDB influxDB = processor.getInfluxDB(testRunner.getProcessContext());

        Mockito.verify(influxDB, Mockito.times(1)).setLogLevel(Mockito.eq(InfluxDB.LogLevel.FULL));
    }

    @Test
    public void logLevelNotDefined() {

        testRunner.setProperty(PutInfluxDBRecord.LOG_LEVEL, "");

        InfluxDB influxDB = processor.getInfluxDB(testRunner.getProcessContext());

        Mockito.verify(influxDB, Mockito.times(1)).setLogLevel(Mockito.eq(InfluxDB.LogLevel.NONE));
    }

    @Test
    public void logLevelUnsupportedName() {

        testRunner.setProperty(PutInfluxDBRecord.LOG_LEVEL, "wrong_name");

        InfluxDB influxDB = processor.getInfluxDB(testRunner.getProcessContext());

        Mockito.verify(influxDB, Mockito.times(1)).setLogLevel(Mockito.eq(InfluxDB.LogLevel.NONE));
    }

    @Test
    public void consistencyLevel() {

        testRunner.setProperty(PutInfluxDBRecord.CONSISTENCY_LEVEL, InfluxDB.ConsistencyLevel.QUORUM.toString());

        InfluxDB influxDB = processor.getInfluxDB(testRunner.getProcessContext());

        Mockito.verify(influxDB, Mockito.times(1)).setConsistency(Mockito.eq(InfluxDB.ConsistencyLevel.QUORUM));
    }

    @Test
    public void consistencyLevelNotDefined() {

        testRunner.setProperty(PutInfluxDBRecord.CONSISTENCY_LEVEL, "");

        InfluxDB influxDB = processor.getInfluxDB(testRunner.getProcessContext());

        Mockito.verify(influxDB, Mockito.times(1)).setConsistency(Mockito.eq(InfluxDB.ConsistencyLevel.ONE));
    }

    @Test
    public void consistencyLevelUnsupportedName() {

        testRunner.setProperty(PutInfluxDBRecord.CONSISTENCY_LEVEL, "wrong_name");

        InfluxDB influxDB = processor.getInfluxDB(testRunner.getProcessContext());

        Mockito.verify(influxDB, Mockito.times(1)).setConsistency(Mockito.eq(InfluxDB.ConsistencyLevel.ONE));
    }

    @Test
    public void enableBatching() {

        testRunner.setProperty(PutInfluxDBRecord.ENABLE_BATCHING, Boolean.TRUE.toString());

        InfluxDB influxDB = processor.getInfluxDB(testRunner.getProcessContext());

        ArgumentCaptor<BatchOptions> captor = ArgumentCaptor.forClass(BatchOptions.class);

        Mockito.verify(influxDB, Mockito.times(0)).disableBatch();
        Mockito.verify(influxDB, Mockito.times(0)).enableBatch();
        Mockito.verify(influxDB, Mockito.times(1)).enableBatch(captor.capture());

        BatchOptions batchOptions = captor.getValue();

        // default Batch Options settings
        Assert.assertEquals(1000, batchOptions.getFlushDuration());
        Assert.assertEquals(1000, batchOptions.getActions());
        Assert.assertEquals(0, batchOptions.getJitterDuration());
        Assert.assertEquals(10000, batchOptions.getBufferLimit());
        Assert.assertEquals(InfluxDB.ConsistencyLevel.ONE, batchOptions.getConsistency());
    }

    @Test
    public void configureBatching() {

        testRunner.setProperty(PutInfluxDBRecord.ENABLE_BATCHING, Boolean.TRUE.toString());
        testRunner.setProperty(PutInfluxDBRecord.CONSISTENCY_LEVEL, InfluxDB.ConsistencyLevel.QUORUM.toString());
        testRunner.setProperty(PutInfluxDBRecord.BATCH_FLUSH_DURATION, "10 sec");
        testRunner.setProperty(PutInfluxDBRecord.BATCH_ACTIONS, "2000");
        testRunner.setProperty(PutInfluxDBRecord.BATCH_JITTER_DURATION, "3 sec");
        testRunner.setProperty(PutInfluxDBRecord.BATCH_BUFFER_LIMIT, "40000");

        InfluxDB influxDB = processor.getInfluxDB(testRunner.getProcessContext());

        ArgumentCaptor<BatchOptions> captor = ArgumentCaptor.forClass(BatchOptions.class);

        Mockito.verify(influxDB, Mockito.times(0)).disableBatch();
        Mockito.verify(influxDB, Mockito.times(0)).enableBatch();
        Mockito.verify(influxDB, Mockito.times(1)).enableBatch(captor.capture());

        BatchOptions batchOptions = captor.getValue();

        // Batch Options settings
        Assert.assertEquals(10000, batchOptions.getFlushDuration());
        Assert.assertEquals(2000, batchOptions.getActions());
        Assert.assertEquals(3000, batchOptions.getJitterDuration());
        Assert.assertEquals(40000, batchOptions.getBufferLimit());
        Assert.assertEquals(InfluxDB.ConsistencyLevel.QUORUM, batchOptions.getConsistency());
    }

    @Test
    public void writeOptions() throws PutInfluxDBRecord.IllegalConfigurationException {

        WriteOptions writeOptions = processor.writeOptions(testRunner.getProcessContext(), null);

        // Write Options
        Assert.assertNotNull(writeOptions);

        // Database
        Assert.assertEquals("nifi-database", writeOptions.getDatabase());

        // Retention Policy
        Assert.assertEquals("autogen", writeOptions.getRetentionPolicy());

        // Timestamp field
        Assert.assertEquals("timestamp", writeOptions.getTimestamp());

        // Timestamp precision
        Assert.assertEquals(TimeUnit.NANOSECONDS, writeOptions.getPrecision());

        // Measurement
        Assert.assertEquals("nifi-measurement", writeOptions.getMeasurement());

        // Fields
        Assert.assertEquals(1, writeOptions.getFields().size());
        Assert.assertEquals("nifi-field", writeOptions.getFields().get(0));

        // Missing Fields
        Assert.assertEquals(MissingItemsBehaviour.IGNORE, writeOptions.getMissingFields());

        // Tags
        Assert.assertEquals(1, writeOptions.getTags().size());
        Assert.assertEquals("tags", writeOptions.getTags().get(0));

        // Missing Tags
        Assert.assertEquals(MissingItemsBehaviour.IGNORE, writeOptions.getMissingTags());

        // Complex fields behaviour
        Assert.assertEquals(ComplexFieldBehaviour.TEXT, writeOptions.getComplexFieldBehaviour());

        // Null Field Behavior
        Assert.assertEquals(NullValueBehaviour.IGNORE, writeOptions.getNullValueBehaviour());
    }

    @Test
    public void timestamp() throws PutInfluxDBRecord.IllegalConfigurationException {

        testRunner.setProperty(PutInfluxDBRecord.TIMESTAMP_FIELD, "createdAt");

        WriteOptions writeOptions = processor.writeOptions(testRunner.getProcessContext(), null);

        Assert.assertEquals("createdAt", writeOptions.getTimestamp());
    }

    @Test
    public void timestampOverFlowFileAttributes() throws PutInfluxDBRecord.IllegalConfigurationException {

        ProcessSession processSession = testRunner.getProcessSessionFactory().createSession();

        FlowFile flowFile = processSession.create();

        Map<String, String> props = new HashMap<>();
        props.put("createdProperty", "createdTimestamp");

        flowFile = processSession.putAllAttributes(flowFile, props);

        testRunner.setProperty(PutInfluxDBRecord.TIMESTAMP_FIELD, "${createdProperty}");

        WriteOptions writeOptions = processor.writeOptions(testRunner.getProcessContext(), flowFile);

        Assert.assertEquals("createdTimestamp", writeOptions.getTimestamp());
    }

    @Test
    public void timestampPrecision() throws PutInfluxDBRecord.IllegalConfigurationException {

        testRunner.setProperty(PutInfluxDBRecord.TIMESTAMP_PRECISION, TimeUnit.MINUTES.name());

        WriteOptions writeOptions = processor.writeOptions(testRunner.getProcessContext(), null);

        Assert.assertEquals(TimeUnit.MINUTES, writeOptions.getPrecision());
    }

    @Test
    public void measurement() throws PutInfluxDBRecord.IllegalConfigurationException {

        testRunner.setProperty(PutInfluxDBRecord.MEASUREMENT, "another-measurement");

        WriteOptions writeOptions = processor.writeOptions(testRunner.getProcessContext(), null);

        Assert.assertEquals("another-measurement", writeOptions.getMeasurement());
    }

    @Test
    public void measurementEmpty() throws PutInfluxDBRecord.IllegalConfigurationException {

        testRunner.setProperty(PutInfluxDBRecord.MEASUREMENT, "");

        expectedException.expect(new TypeOfExceptionMatcher<>(PutInfluxDBRecord.IllegalConfigurationException.class));

        processor.writeOptions(testRunner.getProcessContext(), null);
    }

    @Test
    public void fields() throws PutInfluxDBRecord.IllegalConfigurationException {

        testRunner.setProperty(PutInfluxDBRecord.FIELDS, "user-id, user-screen-name ");

        WriteOptions writeOptions = processor.writeOptions(testRunner.getProcessContext(), null);

        Assert.assertEquals(2, writeOptions.getFields().size());
        Assert.assertEquals("user-id", writeOptions.getFields().get(0));
        Assert.assertEquals("user-screen-name", writeOptions.getFields().get(1));
    }

    @Test
    public void fieldsTrailingComma() throws PutInfluxDBRecord.IllegalConfigurationException {

        testRunner.setProperty(PutInfluxDBRecord.FIELDS, "user-id, ");

        WriteOptions writeOptions = processor.writeOptions(testRunner.getProcessContext(), null);

        Assert.assertEquals(1, writeOptions.getFields().size());
        Assert.assertEquals("user-id", writeOptions.getFields().get(0));
    }

    @Test
    public void fieldsEmpty() throws PutInfluxDBRecord.IllegalConfigurationException {

        testRunner.setProperty(PutInfluxDBRecord.FIELDS, " ");

        expectedException.expect(new TypeOfExceptionMatcher<>(PutInfluxDBRecord.IllegalConfigurationException.class));

        processor.writeOptions(testRunner.getProcessContext(), null);
    }

    @Test
    public void missingFields() throws PutInfluxDBRecord.IllegalConfigurationException {

        testRunner.setProperty(PutInfluxDBRecord.MISSING_FIELD_BEHAVIOR, MissingItemsBehaviour.FAIL.name());

        WriteOptions writeOptions = processor.writeOptions(testRunner.getProcessContext(), null);

        Assert.assertEquals(MissingItemsBehaviour.FAIL, writeOptions.getMissingFields());
    }

    @Test
    public void missingFieldsUnsupported() throws PutInfluxDBRecord.IllegalConfigurationException {

        testRunner.setProperty(PutInfluxDBRecord.MISSING_FIELD_BEHAVIOR, "wrong_name");

        WriteOptions writeOptions = processor.writeOptions(testRunner.getProcessContext(), null);

        Assert.assertEquals(MissingItemsBehaviour.IGNORE, writeOptions.getMissingFields());
    }

    @Test
    public void tags() throws PutInfluxDBRecord.IllegalConfigurationException {

        testRunner.setProperty(PutInfluxDBRecord.TAGS, "lang,keyword");

        WriteOptions writeOptions = processor.writeOptions(testRunner.getProcessContext(), null);

        Assert.assertEquals(2, writeOptions.getTags().size());
        Assert.assertEquals("lang", writeOptions.getTags().get(0));
        Assert.assertEquals("keyword", writeOptions.getTags().get(1));
    }

    @Test
    public void missingTags() throws PutInfluxDBRecord.IllegalConfigurationException {

        testRunner.setProperty(PutInfluxDBRecord.MISSING_TAG_BEHAVIOR, MissingItemsBehaviour.FAIL.name());

        WriteOptions writeOptions = processor.writeOptions(testRunner.getProcessContext(), null);

        Assert.assertEquals(MissingItemsBehaviour.FAIL, writeOptions.getMissingTags());
    }

    @Test
    public void complexFieldBehaviour() throws PutInfluxDBRecord.IllegalConfigurationException {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, ComplexFieldBehaviour.IGNORE.name());

        WriteOptions writeOptions = processor.writeOptions(testRunner.getProcessContext(), null);

        Assert.assertEquals(ComplexFieldBehaviour.IGNORE, writeOptions.getComplexFieldBehaviour());
    }

    @Test
    public void complexFieldBehaviourUnsupported() throws PutInfluxDBRecord.IllegalConfigurationException {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, "wrong_name");

        WriteOptions writeOptions = processor.writeOptions(testRunner.getProcessContext(), null);

        Assert.assertEquals(ComplexFieldBehaviour.TEXT, writeOptions.getComplexFieldBehaviour());
    }

    @Test
    public void nullValueBehavior() throws PutInfluxDBRecord.IllegalConfigurationException {

        testRunner.setProperty(PutInfluxDBRecord.NULL_VALUE_BEHAVIOR, NullValueBehaviour.FAIL.name());

        WriteOptions writeOptions = processor.writeOptions(testRunner.getProcessContext(), null);

        Assert.assertEquals(NullValueBehaviour.FAIL, writeOptions.getNullValueBehaviour());
    }
}
