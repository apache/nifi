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

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.nifi.avro.AvroReader;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processors.gcp.AbstractGCPProcessor;
import org.apache.nifi.processors.gcp.ProxyAwareTransportFactory;
import org.apache.nifi.processors.gcp.credentials.factory.CredentialPropertyDescriptors;
import org.apache.nifi.processors.gcp.credentials.factory.CredentialsFactory;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.nifi.processors.gcp.bigquery.PutBigQuery.BATCH_TYPE;
import static org.apache.nifi.processors.gcp.bigquery.PutBigQuery.STREAM_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Restructured and enhanced version of the existing PutBigQuery Integration Tests. The underlying classes are deprecated so that tests will be removed as well in the future
 *
 */
public class PutBigQueryIT {

    private static final String CONTROLLER_SERVICE = "GCPCredentialsService";
    private static final String PROJECT_ID = System.getProperty("test.gcp.project.id", "nifi");
    private static final String SERVICE_ACCOUNT_JSON = System.getProperty("test.gcp.service.account", "/path/to/service/account.json");

    private static BigQuery bigquery;
    private static Dataset dataset;
    private static TestRunner runner;

    private static final CredentialsFactory credentialsProviderFactory = new CredentialsFactory();

    @BeforeAll
    public static void beforeClass() throws IOException {
        final Map<PropertyDescriptor, String> propertiesMap = new HashMap<>();
        propertiesMap.put(CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE, SERVICE_ACCOUNT_JSON);
        Credentials credentials = credentialsProviderFactory.getGoogleCredentials(propertiesMap, new ProxyAwareTransportFactory(null));

        BigQueryOptions bigQueryOptions = BigQueryOptions.newBuilder()
            .setProjectId(PROJECT_ID)
            .setCredentials(credentials)
            .build();

        bigquery = bigQueryOptions.getService();

        DatasetInfo datasetInfo = DatasetInfo.newBuilder(RemoteBigQueryHelper.generateDatasetName()).build();
        dataset = bigquery.create(datasetInfo);
    }

    @AfterAll
    public static void afterClass() {
        bigquery.delete(dataset.getDatasetId(), BigQuery.DatasetDeleteOption.deleteContents());
    }


    protected TestRunner setCredentialsControllerService(TestRunner runner) throws InitializationException {
        final GCPCredentialsControllerService credentialsControllerService = new GCPCredentialsControllerService();

        final Map<String, String> propertiesMap = new HashMap<>();
        propertiesMap.put(CredentialPropertyDescriptors.SERVICE_ACCOUNT_JSON_FILE.getName(), SERVICE_ACCOUNT_JSON);

        runner.addControllerService(CONTROLLER_SERVICE, credentialsControllerService, propertiesMap);
        runner.enableControllerService(credentialsControllerService);
        runner.assertValid(credentialsControllerService);

        return runner;
    }

    private Schema schema;

    @BeforeEach
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(PutBigQuery.class);
        runner = setCredentialsControllerService(runner);
        runner.setProperty(AbstractGCPProcessor.GCP_CREDENTIALS_PROVIDER_SERVICE, CONTROLLER_SERVICE);
        runner.setProperty(AbstractGCPProcessor.PROJECT_ID, PROJECT_ID);
    }

    @Test
    public void testStreamingNoError() throws Exception {
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
    public void testStreamingFullError() throws Exception {
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
    public void testStreamingPartialError() throws Exception {
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
    public void testStreamingNoErrorWithDate() throws Exception {
        String tableName = prepareTable(STREAM_TYPE);
        addRecordReaderWithSchema("src/test/resources/bigquery/schema-correct-data-with-date.avsc");

        runner.enqueue(Paths.get("src/test/resources/bigquery/streaming-correct-data-with-date.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(PutBigQuery.REL_SUCCESS).get(0).assertAttributeEquals(BigQueryAttributes.JOB_NB_RECORDS_ATTR, "2");

        assertStreamingData(tableName, true, false);

        deleteTable(tableName);
    }

    @Test
    public void testStreamingNoErrorWithDateFormat() throws Exception {
        String tableName = prepareTable(STREAM_TYPE);

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);
        final String recordSchema = new String(Files.readAllBytes(Paths.get("src/test/resources/bigquery/schema-correct-data-with-date.avsc")));
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, recordSchema);
        runner.setProperty(jsonReader, DateTimeUtils.DATE_FORMAT, "MM/dd/yyyy");
        runner.setProperty(jsonReader, DateTimeUtils.TIME_FORMAT, "HH:mm:ss");
        runner.setProperty(jsonReader, DateTimeUtils.TIMESTAMP_FORMAT, "MM-dd-yyyy HH:mm:ss Z");
        runner.enableControllerService(jsonReader);

        runner.setProperty(BigQueryAttributes.RECORD_READER_ATTR, "reader");

        runner.enqueue(Paths.get("src/test/resources/bigquery/streaming-correct-data-with-date-formatted.json"));

        runner.run();
        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(PutBigQuery.REL_SUCCESS).get(0).assertAttributeEquals(BigQueryAttributes.JOB_NB_RECORDS_ATTR, "2");

        assertStreamingData(tableName, false, true);

        deleteTable(tableName);
    }

    @Test
    public void testBatchSmallPayload() throws Exception {
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
    public void testQueryBatchBadRecord() throws Exception {
        String tableName = prepareTable(BATCH_TYPE);
        addRecordReader();

        String str = "{\"field_1\":\"Daniel is great\"}\r\n";
        runner.enqueue(new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8)));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractBigQueryProcessor.REL_FAILURE, 1);

        deleteTable(tableName);
    }

    @Test
    public void testBatchLargePayload() throws InitializationException, IOException {
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

    @Test
    public void testAvroDecimalType() throws InitializationException, IOException {
        String tableName = UUID.randomUUID().toString();
        TableId tableId = TableId.of(dataset.getDatasetId().getDataset(), tableName);
        Field avrodecimal = Field.newBuilder("avrodecimal", StandardSQLTypeName.BIGNUMERIC).setMode(Field.Mode.NULLABLE).build();

        // Table schema definition
        schema = Schema.of(avrodecimal);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        // create table
        bigquery.create(tableInfo);

        runner.setProperty(BigQueryAttributes.DATASET_ATTR, dataset.getDatasetId().getDataset());
        runner.setProperty(BigQueryAttributes.TABLE_NAME_ATTR, tableName);
        runner.setProperty(PutBigQuery.TRANSFER_TYPE, BATCH_TYPE);

        AvroReader reader = new AvroReader();
        runner.addControllerService("reader", reader);

        final String recordSchema = new String(Files.readAllBytes(Paths.get("src/test/resources/bigquery/avrodecimal.avsc")));
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_TEXT, recordSchema);

        runner.enableControllerService(reader);
        runner.setProperty(BigQueryAttributes.RECORD_READER_ATTR, "reader");

        runner.enqueue(Paths.get("src/test/resources/bigquery/avrodecimal.avro"));

        runner.run();
        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS, 1);

        TableResult result = bigquery.listTableData(dataset.getDatasetId().getDataset(), tableName, schema);
        Iterator<FieldValueList> iterator = result.getValues().iterator();
        FieldValueList firstElt = iterator.next();
        assertEquals(firstElt.get(0).getNumericValue().intValue(), 0);

        deleteTable(tableName);
    }

    @Test
    public void testAvroFloatType() throws InitializationException, IOException {
        String tableName = UUID.randomUUID().toString();
        TableId tableId = TableId.of(dataset.getDatasetId().getDataset(), tableName);
        Field avrofloat = Field.newBuilder("avrofloat", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.NULLABLE).build();

        // Table schema definition
        schema = Schema.of(avrofloat);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        // create table
        bigquery.create(tableInfo);

        runner.setProperty(BigQueryAttributes.DATASET_ATTR, dataset.getDatasetId().getDataset());
        runner.setProperty(BigQueryAttributes.TABLE_NAME_ATTR, tableName);
        runner.setProperty(PutBigQuery.TRANSFER_TYPE, BATCH_TYPE);

        AvroReader reader = new AvroReader();
        runner.addControllerService("reader", reader);

        final String recordSchema = new String(Files.readAllBytes(Paths.get("src/test/resources/bigquery/avrofloat.avsc")));
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_TEXT, recordSchema);

        runner.enableControllerService(reader);
        runner.setProperty(BigQueryAttributes.RECORD_READER_ATTR, "reader");

        runner.enqueue(Paths.get("src/test/resources/bigquery/avrofloat.avro"));

        runner.run();
        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS, 1);

        TableResult result = bigquery.listTableData(dataset.getDatasetId().getDataset(), tableName, schema);
        Iterator<FieldValueList> iterator = result.getValues().iterator();
        FieldValueList firstElt = iterator.next();
        assertEquals(firstElt.get(0).getDoubleValue(), 1.0);

        deleteTable(tableName);
    }

    @Test
    public void testAvroIntType() throws InitializationException, IOException {
        String tableName = UUID.randomUUID().toString();
        TableId tableId = TableId.of(dataset.getDatasetId().getDataset(), tableName);
        Field avrofloat = Field.newBuilder("avroint", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build();

        // Table schema definition
        schema = Schema.of(avrofloat);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        // create table
        bigquery.create(tableInfo);

        runner.setProperty(BigQueryAttributes.DATASET_ATTR, dataset.getDatasetId().getDataset());
        runner.setProperty(BigQueryAttributes.TABLE_NAME_ATTR, tableName);
        runner.setProperty(PutBigQuery.TRANSFER_TYPE, BATCH_TYPE);

        AvroReader reader = new AvroReader();
        runner.addControllerService("reader", reader);

        final String recordSchema = new String(Files.readAllBytes(Paths.get("src/test/resources/bigquery/avroint.avsc")));
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_TEXT, recordSchema);

        runner.enableControllerService(reader);
        runner.setProperty(BigQueryAttributes.RECORD_READER_ATTR, "reader");

        runner.enqueue(Paths.get("src/test/resources/bigquery/avroint.avro"));

        runner.run();
        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS, 1);

        TableResult result = bigquery.listTableData(dataset.getDatasetId().getDataset(), tableName, schema);
        Iterator<FieldValueList> iterator = result.getValues().iterator();
        FieldValueList firstElt = iterator.next();
        assertEquals(firstElt.get(0).getDoubleValue(), 1.0);

        deleteTable(tableName);
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

        Field numeric = Field.newBuilder("numeric", StandardSQLTypeName.NUMERIC).setMode(Field.Mode.NULLABLE).build();
        Field floatc = Field.newBuilder("floatc", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.NULLABLE).build();
        Field json = Field.newBuilder("json", StandardSQLTypeName.JSON).setMode(Field.Mode.NULLABLE).build();

        // Table schema definition
        schema = Schema.of(id, name, alias, addresses, job, birth, numeric, floatc, json);
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
        assertStreamingData(tableName, false, false);
    }

    private void assertStreamingData(String tableName, boolean assertDate, boolean assertDateFormatted) {
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
            FieldValueList johnFields = john.get("birth").getRecordValue();
            FieldValueList janeFields = jane.get("birth").getRecordValue();

            ZonedDateTime johnDateTime = Instant.parse("2021-07-18T12:35:24Z").atZone(ZoneId.systemDefault());
            assertEquals(johnDateTime.toLocalDate().format(DateTimeFormatter.ISO_DATE), johnFields.get(0).getStringValue());
            assertEquals(johnDateTime.toLocalTime().format(DateTimeFormatter.ISO_TIME), johnFields.get(1).getStringValue());
            assertEquals(johnDateTime.toInstant().toEpochMilli(), (johnFields.get(2).getTimestampValue() / 1000));

            ZonedDateTime janeDateTime = Instant.parse("1992-01-01T00:00:00Z").atZone(ZoneId.systemDefault());
            assertEquals(janeDateTime.toLocalDate().format(DateTimeFormatter.ISO_DATE), janeFields.get(0).getStringValue());
            assertEquals(janeDateTime.toLocalTime().format(DateTimeFormatter.ISO_TIME), janeFields.get(1).getStringValue());
            assertEquals(janeDateTime.toInstant().toEpochMilli(), (janeFields.get(2).getTimestampValue() / 1000));
        }

        if (assertDateFormatted) {
            FieldValueList johnFields = john.get("birth").getRecordValue();
            FieldValueList janeFields = jane.get("birth").getRecordValue();

            assertEquals("2021-07-18", johnFields.get(0).getStringValue());
            assertEquals("12:35:24", johnFields.get(1).getStringValue());
            assertEquals(Instant.parse("2021-07-18T12:35:24Z").toEpochMilli(), (johnFields.get(2).getTimestampValue() / 1000));

            assertEquals("1992-01-01", janeFields.get(0).getStringValue());
            assertEquals("00:00:00", janeFields.get(1).getStringValue());
            assertEquals( Instant.parse("1992-01-01T00:00:00Z").toEpochMilli(), (janeFields.get(2).getTimestampValue() / 1000));
        }
    }
}
