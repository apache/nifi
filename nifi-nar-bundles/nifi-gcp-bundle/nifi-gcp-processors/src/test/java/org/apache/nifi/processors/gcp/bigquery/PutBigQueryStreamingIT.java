/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.gcp.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.file.Paths;
import java.util.Iterator;

import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processors.gcp.AbstractGCPProcessor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;

public class PutBigQueryStreamingIT extends AbstractBigQueryIT {

    private Schema schema;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(PutBigQueryStreaming.class);
        runner = setCredentialsControllerService(runner);
        runner.setProperty(AbstractGCPProcessor.GCP_CREDENTIALS_PROVIDER_SERVICE, CONTROLLER_SERVICE);
        runner.setProperty(AbstractGCPProcessor.PROJECT_ID, PROJECT_ID);
    }

    private void createTable(String tableName) {
        TableId tableId = TableId.of(dataset.getDatasetId().getDataset(), tableName);

        // Table field definition
        Field id = Field.newBuilder("id", LegacySQLTypeName.INTEGER).setMode(Mode.REQUIRED).build();
        Field name = Field.newBuilder("name", LegacySQLTypeName.STRING).setMode(Mode.NULLABLE).build();
        Field alias = Field.newBuilder("alias", LegacySQLTypeName.STRING).setMode(Mode.REPEATED).build();

        Field zip = Field.newBuilder("zip", LegacySQLTypeName.STRING).setMode(Mode.NULLABLE).build();
        Field city = Field.newBuilder("city", LegacySQLTypeName.STRING).setMode(Mode.NULLABLE).build();
        Field addresses = Field.newBuilder("addresses", LegacySQLTypeName.RECORD, zip, city).setMode(Mode.REPEATED).build();

        Field position = Field.newBuilder("position", LegacySQLTypeName.STRING).setMode(Mode.NULLABLE).build();
        Field company = Field.newBuilder("company", LegacySQLTypeName.STRING).setMode(Mode.NULLABLE).build();
        Field job = Field.newBuilder("job", LegacySQLTypeName.RECORD, position, company).setMode(Mode.NULLABLE).build();

        // Table schema definition
        schema = Schema.of(id, name, alias, addresses, job);
        TableDefinition tableDefinition = StandardTableDefinition.of(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        // create table
        bigquery.create(tableInfo);
    }

    private void deleteTable(String tableName) {
        TableId tableId = TableId.of(dataset.getDatasetId().getDataset(), tableName);
        bigquery.delete(tableId);
    }

    @Test
    public void PutBigQueryStreamingNoError() throws Exception {
        String tableName = Thread.currentThread().getStackTrace()[1].getMethodName();
        createTable(tableName);

        runner.setProperty(BigQueryAttributes.DATASET_ATTR, dataset.getDatasetId().getDataset());
        runner.setProperty(BigQueryAttributes.TABLE_NAME_ATTR, tableName);

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);
        runner.enableControllerService(jsonReader);

        runner.setProperty(BigQueryAttributes.RECORD_READER_ATTR, "reader");

        runner.enqueue(Paths.get("src/test/resources/bigquery/streaming-correct-data.json"));

        runner.run();
        runner.assertAllFlowFilesTransferred(PutBigQueryStreaming.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(PutBigQueryStreaming.REL_SUCCESS).get(0).assertAttributeEquals(BigQueryAttributes.JOB_NB_RECORDS_ATTR, "2");

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
        assertTrue(john.get("alias").getRepeatedValue().size() == 2);
        assertTrue(john.get("addresses").getRepeatedValue().get(0).getRecordValue().get(0).getStringValue().endsWith("000"));

        deleteTable(tableName);
    }

    @Test
    public void PutBigQueryStreamingFullError() throws Exception {
        String tableName = Thread.currentThread().getStackTrace()[1].getMethodName();
        createTable(tableName);

        runner.setProperty(BigQueryAttributes.DATASET_ATTR, dataset.getDatasetId().getDataset());
        runner.setProperty(BigQueryAttributes.TABLE_NAME_ATTR, tableName);

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);
        runner.enableControllerService(jsonReader);

        runner.setProperty(BigQueryAttributes.RECORD_READER_ATTR, "reader");

        runner.enqueue(Paths.get("src/test/resources/bigquery/streaming-bad-data.json"));

        runner.run();
        runner.assertAllFlowFilesTransferred(PutBigQueryStreaming.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(PutBigQueryStreaming.REL_FAILURE).get(0).assertAttributeEquals(BigQueryAttributes.JOB_NB_RECORDS_ATTR, "0");

        TableResult result = bigquery.listTableData(dataset.getDatasetId().getDataset(), tableName, schema);
        assertFalse(result.getValues().iterator().hasNext());

        deleteTable(tableName);
    }

    @Test
    public void PutBigQueryStreamingPartialError() throws Exception {
        String tableName = Thread.currentThread().getStackTrace()[1].getMethodName();
        createTable(tableName);

        runner.setProperty(BigQueryAttributes.DATASET_ATTR, dataset.getDatasetId().getDataset());
        runner.setProperty(BigQueryAttributes.TABLE_NAME_ATTR, tableName);

        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);
        runner.enableControllerService(jsonReader);

        runner.setProperty(BigQueryAttributes.RECORD_READER_ATTR, "reader");
        runner.setProperty(BigQueryAttributes.SKIP_INVALID_ROWS_ATTR, "true");

        runner.enqueue(Paths.get("src/test/resources/bigquery/streaming-bad-data.json"));

        runner.run();
        runner.assertAllFlowFilesTransferred(PutBigQueryStreaming.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(PutBigQueryStreaming.REL_FAILURE).get(0).assertAttributeEquals(BigQueryAttributes.JOB_NB_RECORDS_ATTR, "1");

        TableResult result = bigquery.listTableData(dataset.getDatasetId().getDataset(), tableName, schema);
        Iterator<FieldValueList> iterator = result.getValues().iterator();

        FieldValueList firstElt = iterator.next();
        assertFalse(iterator.hasNext());
        assertEquals(firstElt.get("name").getStringValue(), "Jane Doe");

        deleteTable(tableName);
    }

}
