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

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.LogMessage;
import org.apache.nifi.util.MockFlowFile;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestPutInfluxDBRecord extends AbstractTestPutInfluxDBRecord {

    @Test
    public void withoutFlowFile() {

        testRunner.run();

        Mockito.verify(influxDB, Mockito.never()).write(Mockito.any(Point.class));

        List<BatchPoints> allValues = pointCapture.getAllValues();
        Assert.assertTrue(allValues.isEmpty());
    }

    @Test
    public void measurementValue() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value");

        testRunner.enqueue("");
        testRunner.run();

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals("nifi-measurement", getMeasurement(points.get(0)));
    }

    @Test
    public void measurementValueByFieldValue() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-measurement", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value", "measurement-name");

        testRunner.enqueue("");
        testRunner.run();

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals("measurement-name", getMeasurement(points.get(0)));
    }

    @Test
    public void addDateField() {

        Date fieldValue = new Date();

        addFieldByType(fieldValue, fieldValue.getTime(), RecordFieldType.DATE);
    }

    @Test
    public void addTimeField() {

        Time fieldValue = new Time(System.currentTimeMillis());

        addFieldByType(fieldValue, fieldValue.getTime(), RecordFieldType.TIME);
    }

    @Test
    public void addTimestampField() {

        Timestamp fieldValue = new Timestamp(System.currentTimeMillis());

        addFieldByType(fieldValue, fieldValue.getTime(), RecordFieldType.TIMESTAMP);
    }

    @Test
    public void addDoubleField() {

        double fieldValue = 123456.78d;

        addFieldByType(fieldValue, RecordFieldType.DOUBLE);
    }

    @Test
    public void addFloatField() {

        float fieldValue = -123456.78f;

        addFieldByType(fieldValue, RecordFieldType.FLOAT);
    }

    @Test
    public void addLongField() {

        long fieldValue = 987654321L;

        addFieldByType(fieldValue, RecordFieldType.LONG);
    }

    @Test
    public void addIntField() {

        int fieldValue = 123;

        addFieldByType(fieldValue, RecordFieldType.INT);
    }

    @Test
    public void addByteField() {

        byte fieldValue = (byte) 5;

        addFieldByType(fieldValue, 5, RecordFieldType.BYTE);
    }

    @Test
    public void addShortField() {

        short fieldValue = (short) 100;

        addFieldByType(fieldValue, 100, RecordFieldType.SHORT);
    }

    @Test
    public void addBigIntField() {

        BigInteger fieldValue = new BigInteger("75000");

        addFieldByType(fieldValue, RecordFieldType.BIGINT);
    }

    @Test
    public void addBooleanField() {

        Boolean fieldValue = Boolean.TRUE;

        //noinspection ConstantConditions
        addFieldByType(fieldValue, RecordFieldType.BOOLEAN);
    }

    @Test
    public void addStringField() {

        String fieldValue = "string value";

        addFieldByType(fieldValue, RecordFieldType.STRING);
    }

    @Test
    public void addNullField() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord((String) null);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(0, points.size());
    }

    @Test
    public void addFieldChar() {

        char fieldValue = 'x';

        addFieldByType(fieldValue, String.valueOf(fieldValue), RecordFieldType.CHAR);
    }

    @Test
    public void multipleFields() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-record-1");
        recordReader.addRecord("nifi-record-2");

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(2, points.size());

        Assert.assertEquals("nifi-record-1", getField("nifi-field", points.get(0)));
        Assert.assertEquals("nifi-record-2", getField("nifi-field", points.get(1)));
    }

    @Test
    public void addMapFields() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.MAP);

        Map<String, Object> fields = new HashMap<>();
        fields.put("nifi-float", 55.5F);
        fields.put("nifi-long", 150L);
        fields.put("nifi-boolean", true);
        fields.put("nifi-string", "string value");

        recordReader.addRecord(fields);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals(55.5F, getField("nifi-float", points.get(0)));
        Assert.assertEquals(150F, getField("nifi-long", points.get(0)));
        Assert.assertEquals(true, getField("nifi-boolean", points.get(0)));
        Assert.assertEquals("string value", getField("nifi-string", points.get(0)));
    }

    @Test
    public void addChoiceField() {

        DataType choiceDataType = RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.LONG.getDataType(),
                RecordFieldType.STRING.getDataType());

        addFieldByType("15", 15L, choiceDataType);
    }

    @Test
    public void addArrayFieldText() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.TEXT.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addFieldByType(new Object[]{25L, 55L}, "[25, 55]", dataType);
    }

    @Test
    public void addArrayFieldIgnore() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.IGNORE.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addFieldByType(new Object[]{25L, 55L}, null, dataType, false, PutInfluxDBRecord.REL_SUCCESS);
    }

    @Test
    public void addArrayFieldWarn() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.WARN.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addFieldByType(new Object[]{25L, 55L}, null, dataType, false, PutInfluxDBRecord.REL_SUCCESS);

        List<LogMessage> messages = logger.getWarnMessages();
        Assert.assertEquals(1, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-field; skipping"));
    }

    @Test
    public void addArrayFieldFail() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.FAIL.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addFieldByType(new Object[]{25L, 55L}, null, dataType, false, PutInfluxDBRecord.REL_FAILURE);

        List<LogMessage> messages = logger.getErrorMessages();
        Assert.assertEquals(3, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-field; routing to failure"));
    }

    @Test
    public void addRecordFieldText() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.TEXT.name());

        addFieldRecordType(true, PutInfluxDBRecord.REL_SUCCESS);
    }

    @Test
    public void addRecordFieldIgnore() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.IGNORE.name());

        addFieldRecordType(false, PutInfluxDBRecord.REL_SUCCESS);
    }

    @Test
    public void addRecordFieldWarn() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.WARN.name());

        addFieldRecordType(false, PutInfluxDBRecord.REL_SUCCESS);

        List<LogMessage> messages = logger.getWarnMessages();
        Assert.assertEquals(1, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-field; skipping"));
    }

    @Test
    public void addRecordFieldFail() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.FAIL.name());

        addFieldRecordType(false, PutInfluxDBRecord.REL_FAILURE);

        List<LogMessage> messages = logger.getErrorMessages();
        Assert.assertEquals(3, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-field; routing to failure"));
    }

    @Test
    public void timestampNotDefined() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-record");

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(1, points.size());

        Assert.assertNull(getTimestamp(points.get(0)));
        Assert.assertEquals(TimeUnit.NANOSECONDS, getTimeUnit(points.get(0)));
    }

    @Test
    public void timestampDefined() {

        testRunner.setProperty(PutInfluxDBRecord.TIMESTAMP_FIELD, "nifi-timestamp");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-timestamp", RecordFieldType.LONG);
        recordReader.addRecord("nifi-record", 789456123L);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals(Long.valueOf(789456123L), getTimestamp(points.get(0)));
        Assert.assertEquals(TimeUnit.NANOSECONDS, getTimeUnit(points.get(0)));
    }

    @Test
    public void timestampDefinedButEmpty() {

        testRunner.setProperty(PutInfluxDBRecord.TIMESTAMP_FIELD, "nifi-timestamp");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-timestamp", RecordFieldType.LONG);
        recordReader.addRecord("nifi-record", null);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(1, points.size());

        Assert.assertNull(getTimestamp(points.get(0)));
        Assert.assertEquals(TimeUnit.NANOSECONDS, getTimeUnit(points.get(0)));
    }

    @Test
    public void timestampDefinedAsString() {

        testRunner.setProperty(PutInfluxDBRecord.TIMESTAMP_FIELD, "nifi-timestamp");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-timestamp", RecordFieldType.STRING);
        recordReader.addRecord("nifi-record", "156");

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals(Long.valueOf(156), getTimestamp(points.get(0)));
        Assert.assertEquals(TimeUnit.NANOSECONDS, getTimeUnit(points.get(0)));
    }

    @Test
    public void timestampDefinedAsDate() {

        Date dateValue = new Date();

        testRunner.setProperty(PutInfluxDBRecord.TIMESTAMP_FIELD, "nifi-timestamp");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-timestamp", RecordFieldType.DATE);
        recordReader.addRecord("nifi-record", dateValue);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals(Long.valueOf(dateValue.getTime()), getTimestamp(points.get(0)));
        Assert.assertEquals(TimeUnit.MILLISECONDS, getTimeUnit(points.get(0)));
    }

    @Test
    public void timestampPrecisionDefined() {

        testRunner.setProperty(PutInfluxDBRecord.TIMESTAMP_FIELD, "nifi-timestamp");
        testRunner.setProperty(PutInfluxDBRecord.TIMESTAMP_PRECISION, TimeUnit.HOURS.name());

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-timestamp", RecordFieldType.LONG);
        recordReader.addRecord("nifi-record", 123456L);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals(Long.valueOf(123456L), getTimestamp(points.get(0)));
        Assert.assertEquals(TimeUnit.HOURS, getTimeUnit(points.get(0)));
    }

    @Test
    public void timestampPrecisionIgnoredForDate() {

        Date dateValue = new Date();

        testRunner.setProperty(PutInfluxDBRecord.TIMESTAMP_FIELD, "nifi-timestamp");
        testRunner.setProperty(PutInfluxDBRecord.TIMESTAMP_PRECISION, TimeUnit.HOURS.name());

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-timestamp", RecordFieldType.LONG);
        recordReader.addRecord("nifi-record", dateValue);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(1, points.size());

        Assert.assertEquals(Long.valueOf(dateValue.getTime()), getTimestamp(points.get(0)));
        Assert.assertEquals(TimeUnit.MILLISECONDS, getTimeUnit(points.get(0)));
    }

    @Test
    public void withoutTags() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-record");

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(1, points.size());

        Map<String, String> tags = getTags(points.get(0));
        Assert.assertNotNull(tags);
        Assert.assertEquals(0, tags.size());
    }

    @Test
    public void addStringTag() {

        addTagByType("nifi-tag-value", "nifi-tag-value", RecordFieldType.STRING);
    }

    @Test
    public void addNumberTag() {

        addTagByType(123L, "123", RecordFieldType.LONG);
    }

    @Test
    public void addBooleanTag() {

        addTagByType(true, "true", RecordFieldType.BOOLEAN);
    }

    @Test
    public void addDateTag() throws ParseException {

        Instant instant = Instant.ofEpochMilli(0L);

        addTagByType(Date.from(instant), "1970-01-01", RecordFieldType.DATE);
    }

    @Test
    public void addMoreTags() {

        testRunner.setProperty(PutInfluxDBRecord.TAGS, "nifi-tag1,nifi-tag2");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-tag1", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-tag2", RecordFieldType.STRING);
        recordReader.addRecord("nifi-record", "tag-value1", "tag-value2");

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(1, points.size());

        Map<String, String> tags = getTags(points.get(0));
        Assert.assertNotNull(tags);
        Assert.assertEquals(2, tags.size());
        Assert.assertEquals("tag-value1", tags.get("nifi-tag1"));
        Assert.assertEquals("tag-value2", tags.get("nifi-tag2"));
    }

    @Test
    public void addMapTags() {

        testRunner.setProperty(PutInfluxDBRecord.TAGS, "nifi-tags");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-tags", RecordFieldType.MAP);

        Map<String, String> tags = new HashMap<>();
        tags.put("nifi-tag1", "tag-value1");
        tags.put("nifi-tag2", "tag-value2");

        recordReader.addRecord("nifi-record", tags);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutInfluxDBRecord.REL_SUCCESS, 1);

        List<Point> points = pointCapture.getValue().getPoints();
        Assert.assertEquals(1, points.size());

        Map<String, String> pointTags = getTags(points.get(0));
        Assert.assertNotNull(pointTags);
        Assert.assertEquals(2, pointTags.size());
        Assert.assertEquals("tag-value1", pointTags.get("nifi-tag1"));
        Assert.assertEquals("tag-value2", pointTags.get("nifi-tag2"));
    }

    @Test
    public void addChoiceTag() {

        DataType choiceDataType = RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.LONG.getDataType(),
                RecordFieldType.STRING.getDataType());

        addTagByType("15", "15", choiceDataType);
    }

    @Test
    public void addArrayTagText() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.TEXT.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addTagByType(new Object[]{25L, 55L}, "[25, 55]", dataType);
    }

    @Test
    public void addArrayTagIgnore() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.IGNORE.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addTagByType(new Object[]{25L, 55L}, null, dataType, false, PutInfluxDBRecord.REL_SUCCESS);
    }

    @Test
    public void addArrayTagWarn() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.WARN.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addTagByType(new Object[]{25L, 55L}, null, dataType, false, PutInfluxDBRecord.REL_SUCCESS);

        List<LogMessage> messages = logger.getWarnMessages();
        Assert.assertEquals(1, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-tag; skipping"));
    }

    @Test
    public void addArrayTagFail() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.FAIL.name());

        DataType dataType = RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.LONG.getDataType());

        addTagByType(new Object[]{25L, 55L}, null, dataType, false, PutInfluxDBRecord.REL_FAILURE);

        List<LogMessage> messages = logger.getErrorMessages();
        Assert.assertEquals(3, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-tag; routing to failure"));
    }

    @Test
    public void addRecordTagText() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.TEXT.name());

        addTagRecordType(true, PutInfluxDBRecord.REL_SUCCESS);
    }

    @Test
    public void addRecordTagIgnore() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.IGNORE.name());

        addTagRecordType(false, PutInfluxDBRecord.REL_SUCCESS);
    }

    @Test
    public void addRecordTagWarn() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.WARN.name());

        addTagRecordType(false, PutInfluxDBRecord.REL_SUCCESS);

        List<LogMessage> messages = logger.getWarnMessages();
        Assert.assertEquals(1, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-tag; skipping"));
    }

    @Test
    public void addRecordTagFail() {

        testRunner.setProperty(PutInfluxDBRecord.COMPLEX_FIELD_BEHAVIOR, WriteOptions.ComplexFieldBehaviour.FAIL.name());

        addTagRecordType(false, PutInfluxDBRecord.REL_FAILURE);

        List<LogMessage> messages = logger.getErrorMessages();
        Assert.assertEquals(3, messages.size());
        Assert.assertTrue(messages.get(0).getMsg().contains("Complex value found for nifi-tag; routing to failure"));
    }

    private void addTagRecordType(@NonNull Boolean isValueExpected,
                                  @NonNull Relationship expectedRelations) {

        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("sub-record-field", RecordFieldType.BOOLEAN.getDataType()));

        SimpleRecordSchema childSchema = new SimpleRecordSchema(fields);
        DataType dataType = RecordFieldType.RECORD.getRecordDataType(childSchema);

        Map<String, Object> subRecordValues = new HashMap<>();
        subRecordValues.put("sub-record-field", true);

        addTagByType(subRecordValues, "{sub-record-field=true}", dataType, isValueExpected, expectedRelations);
    }

    @Test
    public void containsProvenanceReport() {

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value");

        testRunner.enqueue("");
        testRunner.run();

        List<ProvenanceEventRecord> events = testRunner.getProvenanceEvents();

        Assert.assertEquals(1, events.size());

        ProvenanceEventRecord record = events.get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, record.getEventType());
        Assert.assertEquals(processor.getIdentifier(), record.getComponentId());
        Assert.assertEquals("http://localhost:8086/nifi-database", record.getTransitUri());
    }

    @Test
    public void nullValueBehaviourIgnoredTag() {

        testRunner.setProperty(PutInfluxDBRecord.TAGS, "nifi-not-exist-tag");
        testRunner.setProperty(PutInfluxDBRecord.NULL_VALUE_BEHAVIOR, WriteOptions.NullValueBehaviour.IGNORE.name());

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-not-exist-tag", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value", null);

        testRunner.enqueue("");
        testRunner.run();

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutInfluxDBRecord.REL_SUCCESS);
        Assert.assertEquals(1, flowFiles.size());

        Map<String, String> tags = getTags(pointCapture.getValue().getPoints().get(0));
        Assert.assertNotNull(tags);
        Assert.assertEquals(0, tags.size());
    }

    @Test
    public void nullValueBehaviourFailTag() {

        testRunner.setProperty(PutInfluxDBRecord.TAGS, "nifi-not-exist-tag");
        testRunner.setProperty(PutInfluxDBRecord.NULL_VALUE_BEHAVIOR, WriteOptions.NullValueBehaviour.FAIL.name());

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-not-exist-tag", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value", null);

        testRunner.enqueue("");
        testRunner.run();

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutInfluxDBRecord.REL_FAILURE);
        Assert.assertEquals(1, flowFiles.size());

        Assert.assertEquals("Cannot write FlowFile to InfluxDB because the field 'nifi-not-exist-tag' has null value.",
                flowFiles.get(0).getAttribute(PutInfluxDBRecord.INFLUX_DB_ERROR_MESSAGE));
    }

    @Test
    public void nullValueBehaviourIgnoredField() {

        testRunner.setProperty(PutInfluxDBRecord.FIELDS, "nifi-field,nifi-not-exist-field");
        testRunner.setProperty(PutInfluxDBRecord.NULL_VALUE_BEHAVIOR, WriteOptions.NullValueBehaviour.IGNORE.name());

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-not-exist-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value", null);

        testRunner.enqueue("");
        testRunner.run();

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutInfluxDBRecord.REL_SUCCESS);
        Assert.assertEquals(1, flowFiles.size());

        Assert.assertNull(getField("nifi-not-exist-field", pointCapture.getValue().getPoints().get(0)));
    }

    @Test
    public void nullValueBehaviourFailField() {

        testRunner.setProperty(PutInfluxDBRecord.FIELDS, "nifi-field,nifi-not-exist-field");
        testRunner.setProperty(PutInfluxDBRecord.NULL_VALUE_BEHAVIOR, WriteOptions.NullValueBehaviour.FAIL.name());

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-not-exist-field", RecordFieldType.STRING);
        recordReader.addRecord("nifi-field-value", null);

        testRunner.enqueue("");
        testRunner.run();

        List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(PutInfluxDBRecord.REL_FAILURE);
        Assert.assertEquals(1, flowFiles.size());

        Assert.assertEquals("Cannot write FlowFile to InfluxDB because the field 'nifi-not-exist-field' has null value.",
                flowFiles.get(0).getAttribute(PutInfluxDBRecord.INFLUX_DB_ERROR_MESSAGE));
    }

    private void addTagByType(@Nullable final Object tagValue,
                              @Nullable final Object expectedValue,
                              @NonNull final RecordFieldType tagType) {
        addTagByType(tagValue, expectedValue, tagType.getDataType());
    }

    private void addTagByType(@Nullable final Object tagValue,
                              @Nullable final Object expectedValue,
                              @Nullable final DataType dataType) {

        addTagByType(tagValue, expectedValue, dataType, true, PutInfluxDBRecord.REL_SUCCESS);
    }

    private void addTagByType(@Nullable final Object tagValue,
                              @Nullable final Object expectedValue,
                              @Nullable final DataType dataType,
                              @NonNull final Boolean isValueExpected,
                              @NonNull final Relationship expectedRelations) {

        testRunner.setProperty(PutInfluxDBRecord.TAGS, "nifi-tag");

        recordReader.addSchemaField("nifi-field", RecordFieldType.STRING);
        recordReader.addSchemaField("nifi-tag", dataType);
        recordReader.addRecord("nifi-record", tagValue);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(expectedRelations, 1);

        List<BatchPoints> batchPoints = pointCapture.getAllValues();
        if (PutInfluxDBRecord.REL_FAILURE.equals(expectedRelations)) {

            Assert.assertEquals(0, batchPoints.size());

            return;
        }

        Assert.assertEquals(1, batchPoints.size());
        List<Point> points = batchPoints.get(0).getPoints();
        Assert.assertEquals(1, points.size());

        Map<String, String> tags = getTags(points.get(0));
        Assert.assertNotNull(tags);

        if (isValueExpected) {
            Assert.assertEquals(1, tags.size());
            Assert.assertEquals(expectedValue, tags.get("nifi-tag"));
        } else {
            Assert.assertEquals(0, tags.size());
        }
    }

    private void addFieldRecordType(@NonNull final Boolean isValueExpected, @NonNull final Relationship relationship) {

        List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("sub-record-field", RecordFieldType.BOOLEAN.getDataType()));

        SimpleRecordSchema childSchema = new SimpleRecordSchema(fields);
        DataType dataType = RecordFieldType.RECORD.getRecordDataType(childSchema);

        Map<String, Object> subRecordValues = new HashMap<>();
        subRecordValues.put("sub-record-field", true);

        addFieldByType(subRecordValues, "{sub-record-field=true}", dataType, isValueExpected, relationship);
    }

    private void addFieldByType(@Nullable final Object fieldValue,
                                @NonNull final RecordFieldType fieldType) {

        addFieldByType(fieldValue, fieldValue, fieldType);
    }

    private void addFieldByType(@Nullable final Object fieldValue,
                                @Nullable final Object expectedValue,
                                @NonNull final RecordFieldType fieldType) {

        addFieldByType(fieldValue, expectedValue, fieldType.getDataType());
    }

    private void addFieldByType(@Nullable final Object fieldValue,
                                @Nullable final Object expectedValue,
                                @Nullable final DataType dataType) {

        addFieldByType(fieldValue, expectedValue, dataType, true, PutInfluxDBRecord.REL_SUCCESS);
    }

    private void addFieldByType(@Nullable final Object fieldValue,
                                @Nullable final Object expectedValue,
                                @Nullable final DataType dataType,
                                @NonNull final Boolean isValueExpected,
                                @NonNull final Relationship expectedRelations) {

        recordReader.addSchemaField("nifi-field", dataType);
        recordReader.addRecord(fieldValue);

        testRunner.enqueue("");
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(expectedRelations, 1);

        List<BatchPoints> batchPoints = pointCapture.getAllValues();
        if (isValueExpected) {
            Assert.assertEquals(1, batchPoints.size());
            List<Point> points = batchPoints.get(0).getPoints();
            Assert.assertEquals(1, points.size());
            Assert.assertEquals(expectedValue, getField("nifi-field", points.get(0)));
        } else {

            int expectedCount = expectedRelations.equals(PutInfluxDBRecord.REL_FAILURE) ? 0 : 1;

            Assert.assertEquals(expectedCount, batchPoints.size());
        }
    }

    @Nullable
    private String getMeasurement(@NonNull final Point point) {

        return accessFieldValue(point, "measurement");

    }

    @Nullable
    private Long getTimestamp(@NonNull final Point point) {

        return accessFieldValue(point, "time");
    }

    @Nullable
    private TimeUnit getTimeUnit(@NonNull final Point point) {

        return accessFieldValue(point, "precision");
    }

    @Nullable
    private Map<String, String> getTags(@NonNull final Point point) {

        return accessFieldValue(point, "tags");
    }

    @Nullable
    private Object getField(@NonNull final String fieldName,
                            @NonNull final Point point) {

        Map<String, Object> fields = accessFieldValue(point, "fields");
        assert fields != null;

        return fields.get(fieldName);

    }

    @Nullable
    private <T> T accessFieldValue(@NonNull final Point point, @NonNull final String fieldName) {

        try {
            //noinspection unchecked
            return (T) FieldUtils.readField(point, fieldName, true);

        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
