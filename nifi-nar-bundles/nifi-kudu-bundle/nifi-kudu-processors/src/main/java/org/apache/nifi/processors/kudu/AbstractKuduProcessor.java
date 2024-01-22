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

package org.apache.nifi.processors.kudu;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.field.FieldConverter;
import org.apache.nifi.serialization.record.field.StandardFieldConverterRegistry;
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

public abstract class AbstractKuduProcessor extends AbstractProcessor {

    static final PropertyDescriptor KUDU_MASTERS = new Builder()
            .name("Kudu Masters")
            .description("Comma separated addresses of the Kudu masters to connect to.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor KERBEROS_USER_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-user-service")
            .displayName("Kerberos User Service")
            .description("Specifies the Kerberos User Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosUserService.class)
            .required(false)
            .build();

    static final PropertyDescriptor KUDU_OPERATION_TIMEOUT_MS = new Builder()
            .name("kudu-operations-timeout-ms")
            .displayName("Kudu Operation Timeout")
            .description("Default timeout used for user operations (using sessions and scanners)")
            .required(false)
            .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS + "ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor KUDU_KEEP_ALIVE_PERIOD_TIMEOUT_MS = new Builder()
            .name("kudu-keep-alive-period-timeout-ms")
            .displayName("Kudu Keep Alive Period Timeout")
            .description("Default timeout used for user operations")
            .required(false)
            .defaultValue(AsyncKuduClient.DEFAULT_KEEP_ALIVE_PERIOD_MS + "ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private static final int DEFAULT_WORKER_COUNT = Runtime.getRuntime().availableProcessors();
    static final PropertyDescriptor WORKER_COUNT = new Builder()
            .name("worker-count")
            .displayName("Kudu Client Worker Count")
            .description("The maximum number of worker threads handling Kudu client read and write operations. Defaults to the number of available processors.")
            .required(true)
            .defaultValue(Integer.toString(DEFAULT_WORKER_COUNT))
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor KUDU_SASL_PROTOCOL_NAME = new Builder()
            .name("kudu-sasl-protocol-name")
            .displayName("Kudu SASL Protocol Name")
            .description("The SASL protocol name to use for authenticating via Kerberos. Must match the service principal name.")
            .required(false)
            .defaultValue("kudu")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private static final FieldConverter<Object, Timestamp> TIMESTAMP_FIELD_CONVERTER = StandardFieldConverterRegistry.getRegistry().getFieldConverter(Timestamp.class);
    /** Timestamp Pattern overrides default RecordFieldType.TIMESTAMP pattern of yyyy-MM-dd HH:mm:ss with optional microseconds */
    private static final String MICROSECOND_TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss[.SSSSSS]";

    private volatile KuduClient kuduClient;
    private final ReadWriteLock kuduClientReadWriteLock = new ReentrantReadWriteLock();
    private final Lock kuduClientReadLock = kuduClientReadWriteLock.readLock();
    private final Lock kuduClientWriteLock = kuduClientReadWriteLock.writeLock();

    private volatile KerberosUser kerberosUser;

    protected KerberosUser getKerberosUser() {
        return this.kerberosUser;
    }

    protected boolean supportsIgnoreOperations() {
        try {
            return kuduClient.supportsIgnoreOperations();
        } catch (KuduException e) {
            throw new RuntimeException(e);
        }
    }

    protected void createKerberosUserAndOrKuduClient(ProcessContext context) {
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);
        if (kerberosUserService == null) {
            return;
        }

        kerberosUser = kerberosUserService.createKerberosUser();
        kerberosUser.login();
        createKuduClient(context);
    }

    protected void createKuduClient(ProcessContext context) {
        kuduClientWriteLock.lock();
        try {
            if (this.kuduClient != null) {
                try {
                    this.kuduClient.close();
                } catch (KuduException e) {
                    getLogger().error("Couldn't close Kudu client.");
                }
            }

            if (kerberosUser != null) {
                final KerberosAction<KuduClient> kerberosAction = new KerberosAction<>(kerberosUser, () -> buildClient(context), getLogger());
                this.kuduClient = kerberosAction.execute();
            } else {
                this.kuduClient = buildClient(context);
            }
        } finally {
            kuduClientWriteLock.unlock();
        }
    }

    protected KuduClient buildClient(final ProcessContext context) {
        final String masters = context.getProperty(KUDU_MASTERS).evaluateAttributeExpressions().getValue();
        final int operationTimeout = context.getProperty(KUDU_OPERATION_TIMEOUT_MS).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final int adminOperationTimeout = context.getProperty(KUDU_KEEP_ALIVE_PERIOD_TIMEOUT_MS).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final String saslProtocolName = context.getProperty(KUDU_SASL_PROTOCOL_NAME).evaluateAttributeExpressions().getValue();
        final int workerCount = context.getProperty(WORKER_COUNT).asInteger();

        // Create Executor following approach of Executors.newCachedThreadPool() using worker count as maximum pool size
        final int corePoolSize = 0;
        final long threadKeepAliveTime = 60;
        final Executor nioExecutor = new ThreadPoolExecutor(
                corePoolSize,
                workerCount,
                threadKeepAliveTime,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new ClientThreadFactory(getIdentifier())
        );

        return new KuduClient.KuduClientBuilder(masters)
            .defaultAdminOperationTimeoutMs(adminOperationTimeout)
            .defaultOperationTimeoutMs(operationTimeout)
            .saslProtocolName(saslProtocolName)
            .workerCount(workerCount)
            .nioExecutor(nioExecutor)
            .build();
    }

    protected void executeOnKuduClient(Consumer<KuduClient> actionOnKuduClient) {
        kuduClientReadLock.lock();
        try {
            actionOnKuduClient.accept(kuduClient);
        } finally {
            kuduClientReadLock.unlock();
        }
    }

    /**
     * Get the pending errors from the active {@link KuduSession}. This will only be applicable if the flushMode is
     * {@code SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND}.
     * @return  a {@link List} of pending {@link RowError}s
     */
    protected List<RowError> getPendingRowErrorsFromKuduSession(final KuduSession kuduSession) {
        if (kuduSession.getFlushMode() == SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND) {
            return Arrays.asList(kuduSession.getPendingErrors().getRowErrors());
        } else {
            return Collections.EMPTY_LIST;
        }
    }

    protected List<RowError> flushKuduSession(final KuduSession kuduSession) throws KuduException {
        final List<OperationResponse> responses = kuduSession.flush();
        // RowErrors will only be present in the OperationResponses in this case if the flush mode
        // selected is MANUAL_FLUSH. It will be empty otherwise.
        return getRowErrors(responses);
    }

    protected List<RowError> closeKuduSession(final KuduSession kuduSession) throws KuduException {
        final List<OperationResponse> responses = kuduSession.close();
        // RowErrors will only be present in the OperationResponses in this case if the flush mode
        // selected is MANUAL_FLUSH, since the underlying implementation of kuduSession.close() returns
        // the OperationResponses from a flush() call.
        return getRowErrors(responses);
    }

    @OnStopped
    public void shutdown() throws Exception {
        try {
            if (this.kuduClient != null) {
                getLogger().debug("Closing KuduClient");
                this.kuduClient.close();
                this.kuduClient  = null;
            }
        } finally {
            if (kerberosUser != null) {
                kerberosUser.logout();
                kerberosUser = null;
            }
        }
    }

    protected void buildPartialRow(Schema schema, PartialRow row, Record record, List<String> fieldNames, boolean ignoreNull, boolean lowercaseFields) {
        for (String recordFieldName : fieldNames) {
            String colName = recordFieldName;
            if (lowercaseFields) {
                colName = colName.toLowerCase();
            }

            if (!schema.hasColumn(colName)) {
                continue;
            }

            final int columnIndex = schema.getColumnIndex(colName);
            final ColumnSchema colSchema = schema.getColumnByIndex(columnIndex);
            final Type colType = colSchema.getType();

            if (record.getValue(recordFieldName) == null) {
                if (schema.getColumnByIndex(columnIndex).isKey()) {
                    throw new IllegalArgumentException(String.format("Can't set primary key column %s to null ", colName));
                } else if(!schema.getColumnByIndex(columnIndex).isNullable()) {
                    throw new IllegalArgumentException(String.format("Can't set column %s to null ", colName));
                }

                if (!ignoreNull) {
                    row.setNull(colName);
                }
            } else {
                Object value = record.getValue(recordFieldName);
                final Optional<DataType> fieldDataType = record.getSchema().getDataType(recordFieldName);
                final String dataTypeFormat = fieldDataType.map(DataType::getFormat).orElse(null);
                switch (colType) {
                    case BOOL -> row.addBoolean(columnIndex, DataTypeUtils.toBoolean(value, recordFieldName));
                    case INT8 -> row.addByte(columnIndex, DataTypeUtils.toByte(value, recordFieldName));
                    case INT16 -> row.addShort(columnIndex, DataTypeUtils.toShort(value, recordFieldName));
                    case INT32 -> row.addInt(columnIndex, DataTypeUtils.toInteger(value, recordFieldName));
                    case INT64 -> row.addLong(columnIndex, DataTypeUtils.toLong(value, recordFieldName));
                    case UNIXTIME_MICROS -> {
                        final Optional<DataType> optionalDataType = record.getSchema().getDataType(recordFieldName);
                        final Optional<String> optionalPattern = getTimestampPattern(optionalDataType.orElse(null));
                        final Timestamp timestamp = TIMESTAMP_FIELD_CONVERTER.convertField(value, optionalPattern, recordFieldName);
                        row.addTimestamp(columnIndex, timestamp);
                    }
                    case STRING -> row.addString(columnIndex, DataTypeUtils.toString(value, dataTypeFormat));
                    case BINARY -> row.addBinary(columnIndex, DataTypeUtils.toString(value, dataTypeFormat).getBytes());
                    case FLOAT -> row.addFloat(columnIndex, DataTypeUtils.toFloat(value, recordFieldName));
                    case DOUBLE -> row.addDouble(columnIndex, DataTypeUtils.toDouble(value, recordFieldName));
                    case DECIMAL -> row.addDecimal(columnIndex, new BigDecimal(DataTypeUtils.toString(value, dataTypeFormat)));
                    case VARCHAR -> row.addVarchar(columnIndex, DataTypeUtils.toString(value, dataTypeFormat));
                    case DATE -> {
                        final String dateFormat = dataTypeFormat == null ? RecordFieldType.DATE.getDefaultFormat() : dataTypeFormat;
                        row.addDate(columnIndex, getDate(value, recordFieldName, dateFormat));
                    }
                    default -> throw new IllegalStateException(String.format("unknown column type %s", colType));
                }
            }
        }
    }

    /**
     * Get Timestamp Pattern and override Timestamp Record Field pattern with optional microsecond pattern
     *
     * @param dataType Data Type
     * @return Optional Timestamp Pattern
     */
    private Optional<String> getTimestampPattern(final DataType dataType) {
        if (dataType == null) {
            return Optional.empty();
        }

        return Optional.of(RecordFieldType.TIMESTAMP == dataType.getFieldType() ? MICROSECOND_TIMESTAMP_PATTERN : dataType.getFormat());
    }

    /**
     * Get java.sql.Date from Record Field Value with optional parsing when input value is a String
     *
     * @param value Record Field Value
     * @param recordFieldName Record Field Name
     * @param format Date Format Pattern
     * @return Date object or null when value is null
     */
    private Date getDate(final Object value, final String recordFieldName, final String format) {
        final FieldConverter<Object, LocalDate> converter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(LocalDate.class);
        final LocalDate localDate = converter.convertField(value, Optional.ofNullable(format), recordFieldName);
        return Date.valueOf(localDate);
    }

    /**
     * Converts a NiFi DataType to it's equivalent Kudu Type.
     */
    private Type toKuduType(DataType nifiType) {
        return switch (nifiType.getFieldType()) {
            case BOOLEAN -> Type.BOOL;
            case BYTE -> Type.INT8;
            case SHORT -> Type.INT16;
            case INT -> Type.INT32;
            case LONG -> Type.INT64;
            case FLOAT -> Type.FLOAT;
            case DOUBLE -> Type.DOUBLE;
            case DECIMAL -> Type.DECIMAL;
            case TIMESTAMP -> Type.UNIXTIME_MICROS;
            case CHAR, STRING -> Type.STRING;
            case DATE -> Type.DATE;
            default -> throw new IllegalArgumentException(String.format("unsupported type %s", nifiType));
        };
    }

    private ColumnTypeAttributes getKuduTypeAttributes(final DataType nifiType) {
        if (nifiType.getFieldType().equals(RecordFieldType.DECIMAL)) {
            final DecimalDataType decimalDataType = (DecimalDataType) nifiType;
            return new ColumnTypeAttributes.ColumnTypeAttributesBuilder().precision(decimalDataType.getPrecision()).scale(decimalDataType.getScale()).build();
        } else {
            return null;
        }
    }

    /**
     * Based on NiFi field declaration, generates an alter statement to extend table with new column. Note: simply calling
     * {@link AlterTableOptions#addNullableColumn(String, Type)} is not sufficient as it does not cover BigDecimal scale and precision handling.
     *
     * @param columnName Name of the new table column.
     * @param nifiType Type of the field.
     *
     * @return Alter table statement to extend table with the new field.
     */
    protected AlterTableOptions getAddNullableColumnStatement(final String columnName, final DataType nifiType) {
        final AlterTableOptions alterTable = new AlterTableOptions();

        alterTable.addColumn(new ColumnSchema.ColumnSchemaBuilder(columnName, toKuduType(nifiType))
                .nullable(true)
                .typeAttributes(getKuduTypeAttributes(nifiType))
                .build());

        return alterTable;
    }

    private static class ClientThreadFactory implements ThreadFactory {
        private final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();

        private final AtomicInteger threadCount = new AtomicInteger();

        private final String identifier;

        private ClientThreadFactory(final String identifier) {
            this.identifier = identifier;
        }

        /**
         * Create new daemon Thread with custom name
         *
         * @param runnable Runnable
         * @return Created Thread
         */
        @Override
        public Thread newThread(final Runnable runnable) {
            final Thread thread = defaultThreadFactory.newThread(runnable);
            thread.setDaemon(true);
            thread.setName(getName());
            return thread;
        }

        private String getName() {
            return String.format("PutKudu[%s]-client-%d", identifier, threadCount.getAndIncrement());
        }
    }

    private List<RowError> getRowErrors(final List<OperationResponse> responses) {
        return responses.stream()
                .filter(OperationResponse::hasRowError)
                .map(OperationResponse::getRowError)
                .collect(Collectors.toList());
    }
}
