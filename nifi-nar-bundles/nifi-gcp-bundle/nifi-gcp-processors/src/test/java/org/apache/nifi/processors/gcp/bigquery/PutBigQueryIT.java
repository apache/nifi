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

package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processors.gcp.AbstractGCPProcessor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.UUID;

import static org.apache.nifi.processors.gcp.bigquery.PutBigQuery.BATCH_TYPE;
import static org.apache.nifi.processors.gcp.bigquery.PutBigQuery.STREAM_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Restructured and enhanced version of the existing PutBigQuery Integration Tests. The underlying classes are deprecated so that tests will be removed as well in the future
 *
 * @see PutBigQueryStreamingIT
 * @see PutBigQueryBatchIT
 */
public class PutBigQueryIT extends AbstractBigQueryIT {

    private Schema schema;

    @BeforeEach
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(PutBigQuery.class);
        runner = setCredentialsControllerService(runner);
        runner.setProperty(AbstractGCPProcessor.GCP_CREDENTIALS_PROVIDER_SERVICE, CONTROLLER_SERVICE);
        runner.setProperty(AbstractGCPProcessor.PROJECT_ID, PROJECT_ID);
    }

    @Test
    public void PutBigQueryStreamingNoError() throws Exception {
        String tableName = prepareTable(STREAM_TYPE);
        addRecordReader();

        runner.enqueue(Paths.get("src/test/resources/bigquery/streaming-correct-data.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(PutBigQuery.REL_SUCCESS).get(0).assertAttributeEquals(BigQueryAttributes.JOB_NB_RECORDS_ATTR, "2");

        assertStreamingData(tableName);

        deleteTable(tableName);
    }

    @Test
    public void PutBigQueryStreamingFullError() throws Exception {
        String tableName = prepareTable(STREAM_TYPE);
        addRecordReader();

        runner.enqueue(Paths.get("src/test/resources/bigquery/streaming-bad-data.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(PutBigQuery.REL_FAILURE).get(0).assertAttributeEquals(BigQueryAttributes.JOB_NB_RECORDS_ATTR, "0");

        TableResult result = bigquery.listTableData(dataset.getDatasetId().getDataset(), tableName, schema);
        assertFalse(result.getValues().iterator().hasNext());

        deleteTable(tableName);
    }

    @Test
    public void PutBigQueryStreamingPartialError() throws Exception {
        String tableName = prepareTable(STREAM_TYPE);
        addRecordReader();

        runner.setProperty(BigQueryAttributes.SKIP_INVALID_ROWS_ATTR, "true");

        runner.enqueue(Paths.get("src/test/resources/bigquery/streaming-bad-data.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(PutBigQuery.REL_SUCCESS).get(0).assertAttributeEquals(BigQueryAttributes.JOB_NB_RECORDS_ATTR, "1");

        TableResult result = bigquery.listTableData(dataset.getDatasetId().getDataset(), tableName, schema);
        Iterator<FieldValueList> iterator = result.getValues().iterator();

        FieldValueList firstElt = iterator.next();
        assertFalse(iterator.hasNext());
        assertEquals(firstElt.get("name").getStringValue(), "Jane Doe");

        deleteTable(tableName);
    }

    @Test
    public void PutBigQueryStreamingNoErrorWithDate() throws Exception {
        String tableName = prepareTable(STREAM_TYPE);
        addRecordReaderWithSchema("src/test/resources/bigquery/schema-correct-data-with-date.avsc");

        runner.enqueue(Paths.get("src/test/resources/bigquery/streaming-correct-data-with-date.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(PutBigQuery.REL_SUCCESS).get(0).assertAttributeEquals(BigQueryAttributes.JOB_NB_RECORDS_ATTR, "2");

        assertStreamingData(tableName, true);

        deleteTable(tableName);
    }

    @Test
    public void PutBigQueryStreamingNoErrorWithDateFormat() throws Exception {
        String tableName = prepareTable(STREAM_TYPE);

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);
        final String recordSchema = new String(Files.readAllBytes(Paths.get("src/test/resources/bigquery/schema-correct-data-with-date.avsc")));
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, recordSchema);
        runner.setProperty(jsonReader, DateTimeUtils.DATE_FORMAT, "MM/dd/yyyy");
        runner.setProperty(jsonReader, DateTimeUtils.TIME_FORMAT, "HH:mm:ss Z");
        runner.setProperty(jsonReader, DateTimeUtils.TIMESTAMP_FORMAT, "MM-dd-yyyy HH:mm:ss Z");
        runner.enableControllerService(jsonReader);

        runner.setProperty(BigQueryAttributes.RECORD_READER_ATTR, "reader");

        runner.enqueue(Paths.get("src/test/resources/bigquery/streaming-correct-data-with-date-formatted.json"));

        runner.run();
        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(PutBigQuery.REL_SUCCESS).get(0).assertAttributeEquals(BigQueryAttributes.JOB_NB_RECORDS_ATTR, "2");

        assertStreamingData(tableName, true);

        deleteTable(tableName);
    }

    @Test
    public void PutBigQueryBatchSmallPayloadTest() throws Exception {
        String tableName = prepareTable(BATCH_TYPE);
        addRecordReader();

        String str = "{\"field_1\":\"Daniel is great\",\"field_2\":\"Daniel is great\"}\r\n";
        runner.enqueue(new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8)));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(PutBigQuery.REL_SUCCESS).get(0).assertAttributeEquals(BigQueryAttributes.JOB_NB_RECORDS_ATTR, "1");

        deleteTable(tableName);
    }

    @Test
    public void PutBigQueryBatchBadRecordTest() throws Exception {
        String tableName = prepareTable(BATCH_TYPE);
        addRecordReader();

        String str = "{\"field_1\":\"Daniel is great\"}\r\n";
        runner.enqueue(new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8)));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractBigQueryProcessor.REL_FAILURE, 1);

        deleteTable(tableName);
    }

    @Test
    public void PutBigQueryBatchLargePayloadTest() throws InitializationException, IOException {
        String tableName = prepareTable(BATCH_TYPE);
        addRecordReader();
        runner.setProperty(PutBigQuery.APPEND_RECORD_COUNT, "5000");

        String str = "{\"field_1\":\"Daniel is great\",\"field_2\":\"Here's to the crazy ones. The misfits. The rebels. The troublemakers." +
            " The round pegs in the square holes. The ones who see things differently. They're not fond of rules. And they have no respect" +
            " for the status quo. You can quote them, disagree with them, glorify or vilify them. About the only thing you can't do is ignore" +
            " them. Because they change things. They push the human race forward. And while some may see them as the crazy ones, we see genius." +
            " Because the people who are crazy enough to think they can change the world, are the ones who do.\"}\n";
        Path tempFile = Files.createTempFile(tableName, "");
        int recordCount = 100_000;
        try (BufferedWriter writer = Files.newBufferedWriter(tempFile)) {
            for (int i = 0; i < recordCount; i++) {
                writer.write(str);
            }
            writer.flush();
        }

        runner.enqueue(tempFile);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractBigQueryProcessor.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(PutBigQuery.REL_SUCCESS).get(0).assertAttributeEquals(BigQueryAttributes.JOB_NB_RECORDS_ATTR, Integer.toString(recordCount));
    }

    private String prepareTable(AllowableValue transferType) {
        String tableName = UUID.randomUUID().toString();

        if (STREAM_TYPE.equals(transferType)) {
            createTableForStream(tableName);
        } else {
            createTableForBatch(tableName);
        }

        runner.setProperty(BigQueryAttributes.DATASET_ATTR, dataset.getDatasetId().getDataset());
        runner.setProperty(BigQueryAttributes.TABLE_NAME_ATTR, tableName);
        runner.setProperty(PutBigQuery.TRANSFER_TYPE, transferType);

        return tableName;
    }

    private void addRecordReader() throws InitializationException {
        JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);
        runner.enableControllerService(jsonReader);

        runner.setProperty(BigQueryAttributes.RECORD_READER_ATTR, "reader");
    }

    private void addRecordReaderWithSchema(String schema) throws InitializationException, IOException {
        JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);
        runner.setProperty(BigQueryAttributes.RECORD_READER_ATTR, "reader");

        String recordSchema = new String(Files.readAllBytes(Paths.get(schema)));
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, recordSchema);

        runner.enableControllerService(jsonReader);
    }

    private void createTableForStream(String tableName) {
        TableId tableId = TableId.of(dataset.getDatasetId().getDataset(), tableName);

        // Table field definition
        Field id = Field.newBuilder("id", LegacySQLTypeName.INTEGER).setMode(Field.Mode.REQUIRED).build();
        Field name = Field.newBuilder("name", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build();
        Field alias = Field.newBuilder("alias", LegacySQLTypeName.STRING).setMode(Field.Mode.REPEATED).build();

        Field zip = Field.newBuilder("zip", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build();
        Field city = Field.newBuilder("city", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build();
        Field addresses = Field.newBuilder("addresses", LegacySQLTypeName.RECORD, zip, city).setMode(Field.Mode.REPEATED).build();

        Field position = Field.newBuilder("position", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build();
        Field company = Field.newBuilder("company", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build();
        Field job = Field.newBuilder("job", LegacySQLTypeName.RECORD, position, company).setMode(Field.Mode.NULLABLE).build();

        Field date = Field.newBuilder("date", LegacySQLTypeName.DATE).setMode(Field.Mode.NULLABLE).build();
        Field time = Field.newBuilder("time", LegacySQLTypeName.TIME).setMode(Field.Mode.NULLABLE).build();
        Field full = Field.newBuilder("full", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build();
        Field birth = Field.newBuilder("birth", LegacySQLTypeName.RECORD, date, time, full).setMode(Field.Mode.NULLABLE).build();

        // Table schema definition
        schema = Schema.of(id, name, alias, addresses, job, birth);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        // create table
        bigquery.create(tableInfo);
    }

    private void createTableForBatch(String tableName) {
        TableId tableId = TableId.of(dataset.getDatasetId().getDataset(), tableName);

        // Table field definition
        Field field1 = Field.newBuilder("field_1", LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build();
        Field field2 = Field.newBuilder("field_2", LegacySQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build();
        Field field3 = Field.newBuilder("field_3", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build();

        // Table schema definition
        schema = Schema.of(field1, field2, field3);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        // create table
        bigquery.create(tableInfo);
    }

    private void deleteTable(String tableName) {
        TableId tableId = TableId.of(dataset.getDatasetId().getDataset(), tableName);
        bigquery.delete(tableId);
    }

    private void assertStreamingData(String tableName) {
        assertStreamingData(tableName, false);
    }

    private void assertStreamingData(String tableName, boolean assertDate) {
        TableResult result = bigquery.listTableData(dataset.getDatasetId().getDataset(), tableName, schema);
        Iterator<FieldValueList> iterator = result.getValues().iterator();

        FieldValueList firstElt = iterator.next();
        FieldValueList sndElt = iterator.next();
        assertTrue(firstElt.get("name").getStringValue().endsWith("Doe"));
        assertTrue(sndElt.get("name").getStringValue().endsWith("Doe"));

        FieldValueList john;
        FieldValueList jane;
        john = firstElt.get("name").getStringValue().equals("John Doe") ? firstElt : sndElt;
        jane = firstElt.get("name").getStringValue().equals("Jane Doe") ? firstElt : sndElt;

        assertEquals(jane.get("job").getRecordValue().get(0).getStringValue(), "Director");
        assertEquals(2, john.get("alias").getRepeatedValue().size());
        assertTrue(john.get("addresses").getRepeatedValue().get(0).getRecordValue().get(0).getStringValue().endsWith("000"));

        if (assertDate) {
            long timestampRecordJohn = LocalDateTime.parse("07-18-2021 12:35:24",
                DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss")).atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
            assertEquals(john.get("birth").getRecordValue().get(0).getStringValue(), "2021-07-18");
            assertEquals(john.get("birth").getRecordValue().get(1).getStringValue(), "12:35:24");
            assertEquals((john.get("birth").getRecordValue().get(2).getTimestampValue() / 1000), timestampRecordJohn);

            long timestampRecordJane = LocalDateTime.parse("01-01-1992 00:00:00",
                DateTimeFormatter.ofPattern("MM-dd-yyyy HH:mm:ss")).atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
            assertEquals(jane.get("birth").getRecordValue().get(0).getStringValue(), "1992-01-01");
            assertEquals(jane.get("birth").getRecordValue().get(1).getStringValue(), "00:00:00");
            assertEquals((jane.get("birth").getRecordValue().get(2).getTimestampValue() / 1000) , timestampRecordJane);
        }
    }
}
