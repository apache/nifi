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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.CivilTimeEncoder;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.StorageError;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.bigquery.storage.v1.stub.BigQueryWriteStubSettings;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.Status;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.bigquery.proto.ProtoUtils;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@TriggerSerially
@Tags({"google", "google cloud", "bq", "bigquery"})
@CapabilityDescription("Writes the contents of a FlowFile to a Google BigQuery table. " +
    "The processor is record based so the schema that is used is driven by the RecordReader. Attributes that are not matched to the target schema " +
    "are skipped. Exactly once delivery semantics are achieved via stream offsets.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = BigQueryAttributes.JOB_NB_RECORDS_ATTR, description = BigQueryAttributes.JOB_NB_RECORDS_DESC)
})
public class PutBigQuery extends AbstractBigQueryProcessor {

    static final String STREAM = "STREAM";
    static final String BATCH = "BATCH";
    static final AllowableValue STREAM_TYPE = new AllowableValue(STREAM, STREAM, "Use streaming record handling strategy");
    static final AllowableValue BATCH_TYPE = new AllowableValue(BATCH, BATCH, "Use batching record handling strategy");

    private static final String APPEND_RECORD_COUNT_NAME = "bq.append.record.count";
    private static final String APPEND_RECORD_COUNT_DESC = "The number of records to be appended to the write stream at once. Applicable for both batch and stream types";
    private static final String TRANSFER_TYPE_NAME = "bq.transfer.type";
    private static final String TRANSFER_TYPE_DESC = "Defines the preferred transfer type streaming or batching";

    private static final List<Status.Code> RETRYABLE_ERROR_CODES = Arrays.asList(Status.Code.INTERNAL, Status.Code.ABORTED, Status.Code.CANCELLED);

    private final AtomicReference<Exception> error = new AtomicReference<>();
    private final AtomicInteger appendSuccessCount = new AtomicInteger(0);
    private final Phaser inflightRequestCount = new Phaser(1);
    private BigQueryWriteClient writeClient = null;
    private StreamWriter streamWriter = null;
    private String transferType;
    private String endpoint;
    private int maxRetryCount;
    private int recordBatchCount;

    public static final PropertyDescriptor PROJECT_ID = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(AbstractBigQueryProcessor.PROJECT_ID)
        .required(true)
        .build();

    public static final PropertyDescriptor BIGQUERY_API_ENDPOINT = new PropertyDescriptor.Builder()
        .name("bigquery-api-endpoint")
        .displayName("BigQuery API Endpoint")
        .description("Can be used to override the default BigQuery endpoint. Default is "
                + BigQueryWriteStubSettings.getDefaultEndpoint() + ". "
                + "Format must be hostname:port.")
        .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(true)
        .defaultValue(BigQueryWriteStubSettings.getDefaultEndpoint())
        .build();

    static final PropertyDescriptor TRANSFER_TYPE = new PropertyDescriptor.Builder()
        .name(TRANSFER_TYPE_NAME)
        .displayName("Transfer Type")
        .description(TRANSFER_TYPE_DESC)
        .required(true)
        .defaultValue(STREAM_TYPE.getValue())
        .allowableValues(STREAM_TYPE, BATCH_TYPE)
        .build();

    static final PropertyDescriptor APPEND_RECORD_COUNT = new PropertyDescriptor.Builder()
        .name(APPEND_RECORD_COUNT_NAME)
        .displayName("Append Record Count")
        .description(APPEND_RECORD_COUNT_DESC)
        .required(true)
        .defaultValue("20")
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name(BigQueryAttributes.RECORD_READER_ATTR)
        .displayName("Record Reader")
        .description(BigQueryAttributes.RECORD_READER_DESC)
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();

    public static final PropertyDescriptor SKIP_INVALID_ROWS = new PropertyDescriptor.Builder()
        .name(BigQueryAttributes.SKIP_INVALID_ROWS_ATTR)
        .displayName("Skip Invalid Rows")
        .description(BigQueryAttributes.SKIP_INVALID_ROWS_DESC)
        .required(true)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("false")
        .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(
        GCP_CREDENTIALS_PROVIDER_SERVICE,
        PROJECT_ID,
        BIGQUERY_API_ENDPOINT,
        DATASET,
        TABLE_NAME,
        RECORD_READER,
        TRANSFER_TYPE,
        APPEND_RECORD_COUNT,
        RETRY_COUNT,
        SKIP_INVALID_ROWS,
        PROXY_CONFIGURATION_SERVICE
    );

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);
        transferType = context.getProperty(TRANSFER_TYPE).getValue();
        maxRetryCount = context.getProperty(RETRY_COUNT).asInteger();
        recordBatchCount = context.getProperty(APPEND_RECORD_COUNT).asInteger();
        endpoint = context.getProperty(BIGQUERY_API_ENDPOINT).evaluateAttributeExpressions().getValue();
        writeClient = createWriteClient(getGoogleCredentials(context), ProxyConfiguration.getConfiguration(context));
    }

    @OnUnscheduled
    public void onUnscheduled() {
        writeClient.shutdown();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session)  {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String projectId = context.getProperty(PROJECT_ID).evaluateAttributeExpressions().getValue();
        final String dataset = context.getProperty(DATASET).evaluateAttributeExpressions(flowFile).getValue();
        final String dataTableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final TableName tableName = TableName.of(projectId, dataset, dataTableName);

        WriteStream writeStream;
        Descriptors.Descriptor protoDescriptor;
        TableSchema tableSchema;
        try {
            writeStream = createWriteStream(tableName);
            tableSchema = writeStream.getTableSchema();
            protoDescriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(tableSchema);
            streamWriter = createStreamWriter(writeStream.getName(), protoDescriptor, getGoogleCredentials(context), ProxyConfiguration.getConfiguration(context));
        } catch (Descriptors.DescriptorValidationException | IOException e) {
            getLogger().error("Failed to create Big Query Stream Writer for writing", e);
            context.yield();
            session.rollback();
            return;
        }

        final boolean skipInvalidRows = context.getProperty(SKIP_INVALID_ROWS).evaluateAttributeExpressions(flowFile).asBoolean();
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        int recordNumWritten;
        try {
            try (InputStream in = session.read(flowFile);
                    RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
                recordNumWritten = writeRecordsToStream(reader, protoDescriptor, skipInvalidRows, tableSchema);
            }
            flowFile = session.putAttribute(flowFile, BigQueryAttributes.JOB_NB_RECORDS_ATTR, Integer.toString(recordNumWritten));
        } catch (Exception e) {
            error.set(e);
        } finally {
            finishProcessing(session, flowFile, streamWriter, writeStream.getName(), tableName.toString());
        }
    }

    private int writeRecordsToStream(RecordReader reader, Descriptors.Descriptor descriptor, boolean skipInvalidRows, TableSchema tableSchema) throws Exception {
        Record currentRecord;
        int offset = 0;
        int recordNum = 0;
        ProtoRows.Builder rowsBuilder = ProtoRows.newBuilder();
        while ((currentRecord = reader.nextRecord()) != null) {
            DynamicMessage message = recordToProtoMessage(currentRecord, descriptor, skipInvalidRows, tableSchema);

            if (message == null) {
                continue;
            }

            rowsBuilder.addSerializedRows(message.toByteString());

            if (++recordNum % recordBatchCount == 0) {
                append(new AppendContext(rowsBuilder.build(), offset));
                rowsBuilder = ProtoRows.newBuilder();
                offset = recordNum;
            }
        }

        if (recordNum > offset) {
            append(new AppendContext(rowsBuilder.build(), offset));
        }

        return recordNum;
    }

    private DynamicMessage recordToProtoMessage(Record record, Descriptors.Descriptor descriptor, boolean skipInvalidRows, TableSchema tableSchema) {
        Map<String, Object> valueMap = convertMapRecord(record.toMap());
        DynamicMessage message = null;
        try {
            message = ProtoUtils.createMessage(descriptor, valueMap, tableSchema);
        } catch (RuntimeException e) {
            getLogger().error("Cannot convert record to message", e);
            if (!skipInvalidRows) {
                throw e;
            }
        }

        return message;
    }

    private void append(AppendContext appendContext) throws Exception {
        if (error.get() != null) {
            throw error.get();
        }

        ApiFuture<AppendRowsResponse> future = streamWriter.append(appendContext.getData(), appendContext.getOffset());
        ApiFutures.addCallback(future, new AppendCompleteCallback(appendContext), Runnable::run);

        inflightRequestCount.register();
    }

    private void finishProcessing(ProcessSession session, FlowFile flowFile, StreamWriter streamWriter, String streamName, String parentTable) {
        // Wait for all in-flight requests to complete.
        inflightRequestCount.arriveAndAwaitAdvance();

        // Close the connection to the server.
        streamWriter.close();

        // Verify that no error occurred in the stream.
        if (error.get() != null) {
            getLogger().error("Stream processing failed", error.get());
            flowFile = session.putAttribute(flowFile, BigQueryAttributes.JOB_NB_RECORDS_ATTR, isBatch() ? "0" : String.valueOf(appendSuccessCount.get() * recordBatchCount));
            session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            error.set(null); // set error to null for next execution
        } else {
            if (isBatch()) {
                writeClient.finalizeWriteStream(streamName);

                BatchCommitWriteStreamsRequest commitRequest =
                    BatchCommitWriteStreamsRequest.newBuilder()
                        .setParent(parentTable)
                        .addWriteStreams(streamName)
                        .build();

                BatchCommitWriteStreamsResponse commitResponse = writeClient.batchCommitWriteStreams(commitRequest);

                // If the response does not have a commit time, it means the commit operation failed.
                if (!commitResponse.hasCommitTime()) {
                    for (StorageError err : commitResponse.getStreamErrorsList()) {
                        getLogger().error("Commit Storage Error Code: {} with message {}", err.getCode().name(), err.getErrorMessage());
                    }
                    session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);

                    return;
                }
                getLogger().info("Appended and committed all records successfully.");
            }

            session.transfer(flowFile, REL_SUCCESS);
        }
    }

    class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {

        private final AppendContext appendContext;

        public AppendCompleteCallback(AppendContext appendContext) {
            this.appendContext = appendContext;
        }

        public void onSuccess(AppendRowsResponse response) {
            getLogger().info("Append success with offset: {}", appendContext.getOffset());
            appendSuccessCount.incrementAndGet();
            inflightRequestCount.arriveAndDeregister();
        }

        public void onFailure(Throwable throwable) {
            // If the state is INTERNAL, CANCELLED, or ABORTED, you can retry. For more information,
            // see: https://grpc.github.io/grpc-java/javadoc/io/grpc/StatusRuntimeException.html
            Status status = Status.fromThrowable(throwable);
            if (appendContext.getRetryCount() < maxRetryCount && RETRYABLE_ERROR_CODES.contains(status.getCode())) {
                appendContext.incrementRetryCount();
                try {
                    append(appendContext);
                    inflightRequestCount.arriveAndDeregister();
                    return;
                } catch (Exception e) {
                    getLogger().error("Failed to retry append", e);
                }
            }

            error.compareAndSet(null, Optional.ofNullable(Exceptions.toStorageException(throwable))
                .map(RuntimeException.class::cast)
                .orElse(new RuntimeException(throwable)));

            getLogger().error("Failure during appending data", throwable);
            inflightRequestCount.arriveAndDeregister();
        }
    }

    private WriteStream createWriteStream(TableName tableName) {
        WriteStream.Type type = isBatch() ? WriteStream.Type.PENDING : WriteStream.Type.COMMITTED;
        CreateWriteStreamRequest createWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
            .setParent(tableName.toString())
            .setWriteStream(WriteStream.newBuilder().setType(type).build())
            .build();

        return writeClient.createWriteStream(createWriteStreamRequest);
    }

    protected BigQueryWriteClient createWriteClient(GoogleCredentials credentials, ProxyConfiguration proxyConfiguration) {
        BigQueryWriteClient client;
        try {
            BigQueryWriteSettings.Builder builder = BigQueryWriteSettings.newBuilder();
            builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
            builder.setEndpoint(endpoint);
            builder.setTransportChannelProvider(createTransportChannelProvider(proxyConfiguration));

            client = BigQueryWriteClient.create(builder.build());
        } catch (Exception e) {
            throw new ProcessException("Failed to create Big Query Write Client for writing", e);
        }

        return client;
    }

    protected StreamWriter createStreamWriter(String streamName, Descriptors.Descriptor descriptor, GoogleCredentials credentials, ProxyConfiguration proxyConfiguration) throws IOException {
        ProtoSchema protoSchema = ProtoSchemaConverter.convert(descriptor);

        StreamWriter.Builder builder = StreamWriter.newBuilder(streamName);
        builder.setWriterSchema(protoSchema);
        builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
        builder.setEndpoint(endpoint);
        builder.setChannelProvider(createTransportChannelProvider(proxyConfiguration));

        return builder.build();
    }

    private TransportChannelProvider createTransportChannelProvider(ProxyConfiguration proxyConfiguration) {
        InstantiatingGrpcChannelProvider.Builder builder = InstantiatingGrpcChannelProvider.newBuilder();

        if (proxyConfiguration != null) {
            if (proxyConfiguration.getProxyType() == Proxy.Type.HTTP) {
                builder.setChannelConfigurator(managedChannelBuilder -> managedChannelBuilder.proxyDetector(
                        targetServerAddress -> HttpConnectProxiedSocketAddress.newBuilder()
                                .setTargetAddress((InetSocketAddress) targetServerAddress)
                                .setProxyAddress(new InetSocketAddress(proxyConfiguration.getProxyServerHost(), proxyConfiguration.getProxyServerPort()))
                                .setUsername(proxyConfiguration.getProxyUserName())
                                .setPassword(proxyConfiguration.getProxyUserPassword())
                                .build()
                ));
            } else if (proxyConfiguration.getProxyType() == Proxy.Type.SOCKS) {
                getLogger().warn("Proxy type SOCKS is not supported, the proxy configuration will be ignored");
            }
        }

        return builder.build();
    }

    private boolean isBatch() {
        return BATCH_TYPE.getValue().equals(transferType);
    }

    private static class AppendContext {
        private final ProtoRows data;
        private final long offset;
        private int retryCount;

        AppendContext(ProtoRows data, long offset) {
            this.data = data;
            this.offset = offset;
            this.retryCount = 0;
        }

        public ProtoRows getData() {
            return data;
        }

        public int getRetryCount() {
            return retryCount;
        }

        public void incrementRetryCount() {
            retryCount++;
        }

        public long getOffset() {
            return offset;
        }
    }

    private static Map<String, Object> convertMapRecord(Map<String, Object> map) {
        Map<String, Object> result = new HashMap<>();
        for (String key : map.keySet()) {
            Object obj = map.get(key);
            // BigQuery is not case sensitive on the column names but the protobuf message
            // expect all column names to be lower case
            key = key.toLowerCase();
            if (obj instanceof MapRecord) {
                result.put(key, convertMapRecord(((MapRecord) obj).toMap()));
            } else if (obj instanceof Object[]
                && ((Object[]) obj).length > 0
                && ((Object[]) obj)[0] instanceof MapRecord) {
                List<Map<String, Object>> lmapr = new ArrayList<>();
                for (Object mapr : ((Object[]) obj)) {
                    lmapr.add(convertMapRecord(((MapRecord) mapr).toMap()));
                }
                result.put(key, lmapr);
            } else if (obj instanceof Timestamp) {
                result.put(key, ((Timestamp) obj).getTime() * 1000);
            } else if (obj instanceof Time) {
                LocalTime time = ((Time) obj).toLocalTime();
                org.threeten.bp.LocalTime localTime = org.threeten.bp.LocalTime.of(
                    time.getHour(),
                    time.getMinute(),
                    time.getSecond());
                result.put(key, CivilTimeEncoder.encodePacked64TimeMicros(localTime));
            } else if (obj instanceof Date) {
                result.put(key, (int) ((Date) obj).toLocalDate().toEpochDay());
            } else {
                result.put(key, obj);
            }
        }

        return result;
    }
}
