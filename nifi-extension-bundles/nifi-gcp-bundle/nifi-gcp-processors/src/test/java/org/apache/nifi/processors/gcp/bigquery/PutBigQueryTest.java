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

import com.google.api.core.ApiFutures;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.bigquery.testing.RemoteBigQueryHelper;
import com.google.protobuf.Descriptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.apache.nifi.processors.gcp.bigquery.PutBigQuery.BATCH_TYPE;
import static org.apache.nifi.processors.gcp.bigquery.PutBigQuery.STREAM_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PutBigQuery}.
 */
@ExtendWith(MockitoExtension.class)
public class PutBigQueryTest {

    private static final String FIELD_1_NAME = "id";
    private static final String FIELD_2_NAME = "value";
    private static final String CSV_HEADER = FIELD_1_NAME + "," + FIELD_2_NAME;
    private static final String VALUE_PREFIX = "mySpecialValue";
    private static final String PROJECT_ID = System.getProperty("test.gcp.project.id", "nifi-test-gcp-project");
    private static final Integer RETRIES = 9;

    static final String DATASET = RemoteBigQueryHelper.generateDatasetName();
    @Mock
    protected BigQuery bq;


    private TestRunner runner;

    @Mock
    private BigQueryWriteClient writeClient;

    @Mock
    private WriteStream writeStream;

    @Mock
    private StreamWriter streamWriter;

    @Captor
    private ArgumentCaptor<ProtoRows> protoRowsCaptor;

    @Captor
    private ArgumentCaptor<Long> offsetCaptor;

    @Captor
    private ArgumentCaptor<BatchCommitWriteStreamsRequest> batchCommitRequestCaptor;


    public static TestRunner buildNewRunner(Processor processor) throws Exception {
        final GCPCredentialsService credentialsService = new GCPCredentialsControllerService();

        final TestRunner runner = TestRunners.newTestRunner(processor);
        runner.addControllerService("gcpCredentialsControllerService", credentialsService);
        runner.enableControllerService(credentialsService);

        runner.setProperty(AbstractBigQueryProcessor.GCP_CREDENTIALS_PROVIDER_SERVICE, "gcpCredentialsControllerService");
        runner.setProperty(AbstractBigQueryProcessor.PROJECT_ID, PROJECT_ID);
        runner.setProperty(AbstractBigQueryProcessor.RETRY_COUNT, String.valueOf(RETRIES));

        runner.assertValid(credentialsService);

        return runner;
    }

    @Test
    public void testBiqQueryOptionsConfiguration() throws Exception {
        reset(bq);
        final TestRunner runner = buildNewRunner(getProcessor());

        final AbstractBigQueryProcessor processor = getProcessor();
        final GoogleCredentials mockCredentials = mock(GoogleCredentials.class);

        final BigQueryOptions options = processor.getServiceOptions(runner.getProcessContext(),
            mockCredentials);

        assertEquals(PROJECT_ID, options.getProjectId(), "Project IDs should match");
        assertEquals(RETRIES.intValue(), options.getRetrySettings().getMaxAttempts(), "Retry counts should match");
        assertSame(mockCredentials, options.getCredentials(), "Credentials should be configured correctly");
    }


    public AbstractBigQueryProcessor getProcessor() {
        return new PutBigQuery() {
            @Override
            protected BigQuery getCloudService() {
                return bq;
            }

            @Override
            protected BigQuery getCloudService(final ProcessContext context) {
                return bq;
            }

            @Override
            protected StreamWriter createStreamWriter(String streamName, Descriptors.Descriptor descriptor, GoogleCredentials credentials, ProxyConfiguration proxyConfiguration) {
                return streamWriter;
            }

            @Override
            protected BigQueryWriteClient createWriteClient(GoogleCredentials credentials, ProxyConfiguration proxyConfiguration) {
                return writeClient;
            }
        };
    }

    protected void addRequiredPropertiesToRunner(TestRunner runner) {
        runner.setProperty(PutBigQuery.DATASET, DATASET);
        runner.setProperty(PutBigQuery.TABLE_NAME, "tableName");
        runner.setProperty(PutBigQuery.APPEND_RECORD_COUNT, "999");
        runner.setProperty(PutBigQuery.RETRY_COUNT, "999");
        runner.setProperty(PutBigQuery.RECORD_READER, "csvReader");
        runner.setProperty(PutBigQuery.TRANSFER_TYPE, STREAM_TYPE);
    }

    @BeforeEach
    void setup() throws Exception {
        AbstractBigQueryProcessor processor = getProcessor();
        runner = buildNewRunner(processor);
        decorateWithRecordReader(runner);
        addRequiredPropertiesToRunner(runner);
        runner.assertValid();
    }

    @Test
    void testMandatoryProjectId() {
        runner.removeProperty(PutBigQuery.PROJECT_ID);
        runner.assertNotValid();
    }

    @ParameterizedTest(name = "{index} => csvLineCount={0}, appendRecordCount={1}")
    @MethodSource("generateRecordGroupingParameters")
    void testRecordGrouping(Integer csvLineCount, Integer appendRecordCount) {
        when(writeClient.createWriteStream(isA(CreateWriteStreamRequest.class))).thenReturn(writeStream);
        TableSchema myTableSchema = mockTableSchema(FIELD_1_NAME, TableFieldSchema.Type.STRING, FIELD_2_NAME, TableFieldSchema.Type.STRING);
        when(writeStream.getTableSchema()).thenReturn(myTableSchema);
        when(streamWriter.append(isA(ProtoRows.class), isA(Long.class)))
            .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().setAppendResult(mock(AppendRowsResponse.AppendResult.class)).build()));

        runner.setProperty(PutBigQuery.APPEND_RECORD_COUNT, appendRecordCount.toString());

        runner.enqueue(csvContentWithLines(csvLineCount));
        runner.run();

        Integer expectedAppendCount = (int) Math.ceil( (double) csvLineCount / appendRecordCount);
        verify(streamWriter, times(expectedAppendCount)).append(protoRowsCaptor.capture(), offsetCaptor.capture());
        List<ProtoRows> allValues = protoRowsCaptor.getAllValues();
        List<Long> offsets = offsetCaptor.getAllValues();

        assertEquals(expectedAppendCount, allValues.size());
        for (int i = 0; i < expectedAppendCount - 1; i++) {
            assertEquals(appendRecordCount, allValues.get(i).getSerializedRowsCount());
            assertEquals((long) i * appendRecordCount, offsets.get(i));
            for (int j = 0; j < appendRecordCount; j++) {
                assertTrue(allValues.get(i).getSerializedRowsList().get(j).toString().contains(VALUE_PREFIX + (i * appendRecordCount + j)));
            }
        }

        int lastAppendSize = csvLineCount % appendRecordCount;
        if (lastAppendSize != 0) {
            assertEquals(lastAppendSize, allValues.get(allValues.size() - 1).getSerializedRowsCount());
            for (int j = 0; j < lastAppendSize; j++) {
                assertTrue(allValues.get(allValues.size() - 1).getSerializedRowsList().get(j).toString().contains(VALUE_PREFIX + ((expectedAppendCount - 1) * appendRecordCount + j)));
            }
        }

        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS);
    }

    private static Stream<Arguments> generateRecordGroupingParameters() {
        return Stream.of(
            Arguments.of(2, 3),
            Arguments.of(5, 3),
            Arguments.of(5, 5),
            Arguments.of(11, 5)
        );
    }

    @Test
    void testMultipleFlowFiles() {
        when(writeClient.createWriteStream(isA(CreateWriteStreamRequest.class))).thenReturn(writeStream);
        TableSchema myTableSchema = mockTableSchema(FIELD_1_NAME, TableFieldSchema.Type.STRING, FIELD_2_NAME, TableFieldSchema.Type.STRING);
        when(writeStream.getTableSchema()).thenReturn(myTableSchema);
        when(streamWriter.append(isA(ProtoRows.class), isA(Long.class)))
            .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().setAppendResult(mock(AppendRowsResponse.AppendResult.class)).build()));

        int entityCountFirst = 3;
        int entityCountSecond = 5;
        runner.enqueue(csvContentWithLines(entityCountFirst));
        runner.enqueue(csvContentWithLines(entityCountSecond));

        int iteration = 2;
        runner.run(iteration);

        verify(streamWriter, times(iteration)).append(protoRowsCaptor.capture(), offsetCaptor.capture());
        List<ProtoRows> allValues = protoRowsCaptor.getAllValues();

        assertEquals(iteration, allValues.size());
        assertEquals(entityCountFirst, allValues.get(0).getSerializedRowsCount());
        assertEquals(entityCountSecond, allValues.get(1).getSerializedRowsCount());

        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS, iteration);

        runner.getFlowFilesForRelationship(PutBigQuery.REL_SUCCESS).get(0).assertAttributeEquals(BigQueryAttributes.JOB_NB_RECORDS_ATTR, Integer.toString(entityCountFirst));
        runner.getFlowFilesForRelationship(PutBigQuery.REL_SUCCESS).get(1).assertAttributeEquals(BigQueryAttributes.JOB_NB_RECORDS_ATTR, Integer.toString(entityCountSecond));
    }

    @Test
    void testRetryAndFailureAfterRetryCount() {
        when(writeClient.createWriteStream(isA(CreateWriteStreamRequest.class))).thenReturn(writeStream);
        TableSchema myTableSchema = mockTableSchema(FIELD_1_NAME, TableFieldSchema.Type.STRING, FIELD_2_NAME, TableFieldSchema.Type.STRING);
        when(writeStream.getTableSchema()).thenReturn(myTableSchema);
        when(streamWriter.append(isA(ProtoRows.class), isA(Long.class))).thenReturn(ApiFutures.immediateFailedFuture(new StatusRuntimeException(Status.INTERNAL)));

        int retryCount = 5;
        runner.setProperty(PutBigQuery.RETRY_COUNT, Integer.toString(retryCount));

        runner.enqueue(csvContentWithLines(1));
        runner.run();

        verify(streamWriter, times(retryCount + 1)).append(any(ProtoRows.class), anyLong());

        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_FAILURE);
    }

    @Test
    void testRetryAndSuccessBeforeRetryCount() {
        when(writeClient.createWriteStream(isA(CreateWriteStreamRequest.class))).thenReturn(writeStream);
        TableSchema myTableSchema = mockTableSchema(FIELD_1_NAME, TableFieldSchema.Type.STRING, FIELD_2_NAME, TableFieldSchema.Type.STRING);
        when(writeStream.getTableSchema()).thenReturn(myTableSchema);
        when(streamWriter.append(isA(ProtoRows.class), isA(Long.class)))
            .thenReturn(ApiFutures.immediateFailedFuture(new StatusRuntimeException(Status.INTERNAL)))
            .thenReturn(ApiFutures.immediateFailedFuture(new StatusRuntimeException(Status.INTERNAL)))
            .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().setAppendResult(mock(AppendRowsResponse.AppendResult.class)).build()));

        runner.enqueue(csvContentWithLines(1));
        runner.run();

        verify(streamWriter, times(3)).append(any(ProtoRows.class), anyLong());

        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS);
    }

    @Test
    void testBatch() {
        String streamName = "myStreamName";
        when(writeClient.createWriteStream(isA(CreateWriteStreamRequest.class))).thenReturn(writeStream);

        FinalizeWriteStreamResponse finalizeWriteStreamResponse = mock(FinalizeWriteStreamResponse.class);
        when(writeClient.finalizeWriteStream(streamName)).thenReturn(finalizeWriteStreamResponse);

        BatchCommitWriteStreamsResponse commitResponse = mock(BatchCommitWriteStreamsResponse.class);
        when(commitResponse.hasCommitTime()).thenReturn(true);
        when(writeClient.batchCommitWriteStreams(isA(BatchCommitWriteStreamsRequest.class))).thenReturn(commitResponse);

        TableSchema myTableSchema = mockTableSchema(FIELD_1_NAME, TableFieldSchema.Type.STRING, FIELD_2_NAME, TableFieldSchema.Type.STRING);
        when(writeStream.getTableSchema()).thenReturn(myTableSchema);
        when(writeStream.getName()).thenReturn(streamName);
        when(streamWriter.append(isA(ProtoRows.class), isA(Long.class)))
            .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().setAppendResult(mock(AppendRowsResponse.AppendResult.class)).build()));

        runner.setProperty(PutBigQuery.TRANSFER_TYPE, BATCH_TYPE);
        runner.assertValid();

        runner.enqueue(csvContentWithLines(1));
        runner.run();

        verify(writeClient).finalizeWriteStream(streamName);
        verify(writeClient).batchCommitWriteStreams(batchCommitRequestCaptor.capture());
        BatchCommitWriteStreamsRequest batchCommitRequest = batchCommitRequestCaptor.getValue();
        assertEquals(streamName, batchCommitRequest.getWriteStreams(0));
        verify(streamWriter).append(any(ProtoRows.class), anyLong());

        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS);
    }

    @Test
    void testUnknownColumnSkipped() {
        when(writeClient.createWriteStream(isA(CreateWriteStreamRequest.class))).thenReturn(writeStream);
        TableSchema myTableSchema = mockTableSchema(FIELD_1_NAME, TableFieldSchema.Type.STRING, FIELD_2_NAME, TableFieldSchema.Type.STRING);
        when(writeStream.getTableSchema()).thenReturn(myTableSchema);
        when(streamWriter.append(isA(ProtoRows.class), isA(Long.class)))
            .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().setAppendResult(mock(AppendRowsResponse.AppendResult.class)).build()));

        String unknownProperty = "myUnknownProperty";
        runner.enqueue(CSV_HEADER + ",unknownProperty\nmyId,myValue," + unknownProperty);
        runner.run();

        verify(streamWriter).append(protoRowsCaptor.capture(), offsetCaptor.capture());
        ProtoRows rows = protoRowsCaptor.getValue();
        assertFalse(rows.getSerializedRowsList().get(0).toString().contains(unknownProperty));

        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS);
    }

    @Test
    void testSchema() throws Exception {
        when(writeClient.createWriteStream(isA(CreateWriteStreamRequest.class))).thenReturn(writeStream);

        TableSchema myTableSchema = mockTableSchema(FIELD_1_NAME, TableFieldSchema.Type.INT64, FIELD_2_NAME, TableFieldSchema.Type.STRING);

        when(writeStream.getTableSchema()).thenReturn(myTableSchema);
        when(streamWriter.append(isA(ProtoRows.class), isA(Long.class)))
            .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().setAppendResult(mock(AppendRowsResponse.AppendResult.class)).build()));

        decorateWithRecordReaderWithSchema(runner);

        runner.enqueue(csvContentWithLines(1));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS);
    }

    @Test
    void testSchemaTypeIncompatibility() throws Exception {
        when(writeClient.createWriteStream(isA(CreateWriteStreamRequest.class))).thenReturn(writeStream);

        TableSchema myTableSchema = mockTableSchema(FIELD_1_NAME, TableFieldSchema.Type.STRING, FIELD_2_NAME, TableFieldSchema.Type.STRING);

        when(writeStream.getTableSchema()).thenReturn(myTableSchema);

        decorateWithRecordReaderWithSchema(runner);

        runner.enqueue(csvContentWithLines(1));
        runner.run();

        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_FAILURE);
    }

    @Test
    void testStreamWriterNotInitialized() throws Exception {
        AbstractBigQueryProcessor processor = new PutBigQuery() {
            @Override
            protected BigQuery getCloudService() {
                return bq;
            }

            @Override
            protected BigQuery getCloudService(final ProcessContext context) {
                return bq;
            }

            @Override
            protected StreamWriter createStreamWriter(String streamName, Descriptors.Descriptor descriptor, GoogleCredentials credentials, ProxyConfiguration proxyConfiguration) throws IOException {
                throw new IOException();
            }

            @Override
            protected BigQueryWriteClient createWriteClient(GoogleCredentials credentials, ProxyConfiguration proxyConfiguration) {
                return writeClient;
            }
        };
        runner = buildNewRunner(processor);
        decorateWithRecordReader(runner);
        addRequiredPropertiesToRunner(runner);

        when(writeClient.createWriteStream(isA(CreateWriteStreamRequest.class))).thenReturn(writeStream);
        when(writeStream.getTableSchema()).thenReturn(mock(TableSchema.class));

        runner.enqueue(csvContentWithLines(1));
        runner.run();

        runner.assertQueueNotEmpty();
        runner.assertTransferCount(PutBigQuery.REL_FAILURE, 0);
        runner.assertTransferCount(PutBigQuery.REL_SUCCESS, 0);
    }

    @Test
    void testNextFlowFileProcessedWhenIntermittentErrorResolved() {
        when(writeClient.createWriteStream(isA(CreateWriteStreamRequest.class))).thenReturn(writeStream);
        TableSchema myTableSchema = mockTableSchema(FIELD_1_NAME, TableFieldSchema.Type.STRING, FIELD_2_NAME, TableFieldSchema.Type.STRING);
        when(writeStream.getTableSchema()).thenReturn(myTableSchema);
        when(streamWriter.append(isA(ProtoRows.class), isA(Long.class)))
            .thenReturn(ApiFutures.immediateFailedFuture(new StatusRuntimeException(Status.INTERNAL)))
            .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().setAppendResult(mock(AppendRowsResponse.AppendResult.class)).build()));

        runner.setProperty(PutBigQuery.RETRY_COUNT, "0");

        runner.enqueue(csvContentWithLines(1));
        runner.enqueue(csvContentWithLines(1));
        runner.run(2);

        verify(streamWriter, times(2)).append(any(ProtoRows.class), anyLong());

        runner.assertQueueEmpty();
        runner.assertTransferCount(PutBigQuery.REL_FAILURE, 1);
        runner.assertTransferCount(PutBigQuery.REL_SUCCESS, 1);
    }

    @Test
    void testMapFieldSchema() throws Exception {
        when(writeClient.createWriteStream(isA(CreateWriteStreamRequest.class))).thenReturn(writeStream);

        TableSchema myTableSchema = mockJsonTableSchema();

        when(writeStream.getTableSchema()).thenReturn(myTableSchema);

        when(streamWriter.append(isA(ProtoRows.class), isA(Long.class)))
                .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.newBuilder().setAppendResult(mock(AppendRowsResponse.AppendResult.class)).build()));

        decorateWithJsonRecordReaderWithSchema(runner);
        runner.setProperty(PutBigQuery.RECORD_READER, "jsonReader");

        runner.enqueue(jsonContent());
        runner.run();

        runner.assertAllFlowFilesTransferred(PutBigQuery.REL_SUCCESS);
    }

    private void decorateWithRecordReader(TestRunner runner) throws InitializationException {
        CSVReader csvReader = new CSVReader();
        runner.addControllerService("csvReader", csvReader);
        runner.setProperty(csvReader, CSVUtils.FIRST_LINE_IS_HEADER, "true");
        runner.setProperty(csvReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "csv-header-derived");
        runner.enableControllerService(csvReader);
    }

    private void decorateWithRecordReaderWithSchema(TestRunner runner) throws InitializationException {
        String recordReaderSchema = "{\n" +
            "  \"name\": \"recordFormatName\",\n" +
            "  \"namespace\": \"nifi.examples\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"fields\": [\n" +
            "    { \"name\": \"" + FIELD_1_NAME + "\", \"type\": \"long\" },\n" +
            "    { \"name\": \"" + FIELD_2_NAME + "\", \"type\": \"string\" }\n" +
            "  ]\n" +
            "}";

        CSVReader csvReader = new CSVReader();
        runner.addControllerService("csvReader", csvReader);
        runner.setProperty(csvReader, CSVUtils.FIRST_LINE_IS_HEADER, "true");
        runner.setProperty(csvReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(csvReader, SchemaAccessUtils.SCHEMA_TEXT, recordReaderSchema);
        runner.enableControllerService(csvReader);
    }

    private void decorateWithJsonRecordReaderWithSchema(TestRunner runner) throws InitializationException {
        String recordReaderSchema = """
                {
                  "name": "recordFormatName",
                  "namespace": "nifi.examples",
                  "type": "record",
                  "fields": [
                    {
                      "name": "field",
                      "type": {
                        "type": "map",
                        "values": "string"
                      }
                    }
                  ]
                }""";

        JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("jsonReader", jsonReader);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, recordReaderSchema);
        runner.enableControllerService(jsonReader);
    }

    private TableSchema mockTableSchema(String name1, TableFieldSchema.Type type1, String name2, TableFieldSchema.Type type2) {
        TableSchema myTableSchema = mock(TableSchema.class);

        TableFieldSchema tableFieldSchemaId = mock(TableFieldSchema.class);
        when(tableFieldSchemaId.getMode()).thenReturn(TableFieldSchema.Mode.NULLABLE);
        when(tableFieldSchemaId.getType()).thenReturn(type1);
        when(tableFieldSchemaId.getName()).thenReturn(name1);

        TableFieldSchema tableFieldSchemaValue = mock(TableFieldSchema.class);
        when(tableFieldSchemaValue.getMode()).thenReturn(TableFieldSchema.Mode.NULLABLE);
        when(tableFieldSchemaValue.getType()).thenReturn(type2);
        when(tableFieldSchemaValue.getName()).thenReturn(name2);


        when(myTableSchema.getFieldsList()).thenReturn(Arrays.asList(tableFieldSchemaId, tableFieldSchemaValue));

        return myTableSchema;
    }

    private TableSchema mockJsonTableSchema() {
        TableSchema myTableSchema = mock(TableSchema.class);

        TableFieldSchema keyFieldSchema = mock(TableFieldSchema.class);
        when(keyFieldSchema.getName()).thenReturn("key");
        when(keyFieldSchema.getType()).thenReturn(TableFieldSchema.Type.STRING);
        when(keyFieldSchema.getMode()).thenReturn(TableFieldSchema.Mode.REQUIRED);

        TableFieldSchema valueFieldSchema = mock(TableFieldSchema.class);
        when(valueFieldSchema.getName()).thenReturn("value");
        when(valueFieldSchema.getType()).thenReturn(TableFieldSchema.Type.STRING);
        when(valueFieldSchema.getMode()).thenReturn(TableFieldSchema.Mode.NULLABLE);

        TableFieldSchema tableFieldSchemaId = mock(TableFieldSchema.class);
        when(tableFieldSchemaId.getName()).thenReturn("field");
        when(tableFieldSchemaId.getType()).thenReturn(TableFieldSchema.Type.STRUCT);
        when(tableFieldSchemaId.getMode()).thenReturn(TableFieldSchema.Mode.REPEATED);
        when(tableFieldSchemaId.getFieldsList()).thenReturn(List.of(keyFieldSchema, valueFieldSchema));

        when(myTableSchema.getFieldsList()).thenReturn(List.of(tableFieldSchemaId));

        return myTableSchema;
    }

    private String csvContentWithLines(int lineNum) {
        StringBuilder builder = new StringBuilder();
        builder.append(CSV_HEADER);

        IntStream.range(0, lineNum).forEach(x -> {
            builder.append("\n");
            builder.append(x);
            builder.append(",");
            builder.append(VALUE_PREFIX).append(x);
        });

        return builder.toString();
    }

    private String jsonContent() {
        return """
                {
                  "field": {
                    "FIELD_1": "field_1",
                    "FIELD_2": "field_2",
                    "FIELD_3": "field_3",
                    "FIELD_4": "field_4",
                    "FIELD_5": "field_5"
                  }
                }""";
    }
}
