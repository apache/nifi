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
import org.apache.kudu.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosPasswordUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.StringUtils;

import javax.security.auth.login.LoginException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

public abstract class AbstractKuduProcessor extends AbstractProcessor {

    static final PropertyDescriptor KUDU_MASTERS = new Builder()
            .name("Kudu Masters")
            .description("Comma separated addresses of the Kudu masters to connect to.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials to use for authentication")
            .required(false)
            .identifiesControllerService(KerberosCredentialsService.class)
            .build();

    static final PropertyDescriptor KERBEROS_PRINCIPAL = new PropertyDescriptor.Builder()
            .name("kerberos-principal")
            .displayName("Kerberos Principal")
            .description("The principal to use when specifying the principal and password directly in the processor for authenticating via Kerberos.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor KERBEROS_PASSWORD = new PropertyDescriptor.Builder()
            .name("kerberos-password")
            .displayName("Kerberos Password")
            .description("The password to use when specifying the principal and password directly in the processor for authenticating via Kerberos.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    static final PropertyDescriptor KUDU_OPERATION_TIMEOUT_MS = new Builder()
            .name("kudu-operations-timeout-ms")
            .displayName("Kudu Operation Timeout")
            .description("Default timeout used for user operations (using sessions and scanners)")
            .required(false)
            .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS + "ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor KUDU_KEEP_ALIVE_PERIOD_TIMEOUT_MS = new Builder()
            .name("kudu-keep-alive-period-timeout-ms")
            .displayName("Kudu Keep Alive Period Timeout")
            .description("Default timeout used for user operations")
            .required(false)
            .defaultValue(AsyncKuduClient.DEFAULT_KEEP_ALIVE_PERIOD_MS + "ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final int DEFAULT_WORKER_COUNT = 2 * Runtime.getRuntime().availableProcessors();
    static final PropertyDescriptor WORKER_COUNT = new Builder()
            .name("worker-count")
            .displayName("Kudu Client Worker Count")
            .description("The maximum number of worker threads handling Kudu client read and write operations. Defaults to the number of available processors multiplied by 2.")
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
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

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

    protected void createKerberosUserAndOrKuduClient(ProcessContext context) throws LoginException {
        final KerberosCredentialsService credentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
        final String kerberosPrincipal = context.getProperty(KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue();
        final String kerberosPassword = context.getProperty(KERBEROS_PASSWORD).getValue();

        if (credentialsService != null) {
            kerberosUser = createKerberosKeytabUser(credentialsService.getPrincipal(), credentialsService.getKeytab(), context);
            kerberosUser.login(); // login creates the kudu client as well
        } else if (!StringUtils.isBlank(kerberosPrincipal) && !StringUtils.isBlank(kerberosPassword)) {
            kerberosUser = createKerberosPasswordUser(kerberosPrincipal, kerberosPassword, context);
            kerberosUser.login(); // login creates the kudu client as well
        } else {
            createKuduClient(context);
        }
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
                .defaultOperationTimeoutMs(operationTimeout)
                .defaultSocketReadTimeoutMs(adminOperationTimeout)
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

    protected void flushKuduSession(final KuduSession kuduSession, boolean close, final List<RowError> rowErrors) throws KuduException {
        final List<OperationResponse> responses = close ? kuduSession.close() : kuduSession.flush();

        if (kuduSession.getFlushMode() == SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND) {
            rowErrors.addAll(Arrays.asList(kuduSession.getPendingErrors().getRowErrors()));
        } else {
            responses.stream()
                    .filter(OperationResponse::hasRowError)
                    .map(OperationResponse::getRowError)
                    .forEach(rowErrors::add);
        }
    }

    protected KerberosUser createKerberosKeytabUser(String principal, String keytab, ProcessContext context) {
        return new KerberosKeytabUser(principal, keytab) {
            @Override
            public synchronized void login() throws LoginException {
                if (!isLoggedIn()) {
                    super.login();

                    createKuduClient(context);
                }
            }
        };
    }

    protected KerberosUser createKerberosPasswordUser(String principal, String password, ProcessContext context) {
        return new KerberosPasswordUser(principal, password) {
            @Override
            public synchronized void login() throws LoginException {
                if (!isLoggedIn()) {
                    super.login();

                    createKuduClient(context);
                }
            }
        };
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();

        final boolean kerberosPrincipalProvided = !StringUtils.isBlank(context.getProperty(KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue());
        final boolean kerberosPasswordProvided = !StringUtils.isBlank(context.getProperty(KERBEROS_PASSWORD).getValue());

        if (kerberosPrincipalProvided && !kerberosPasswordProvided) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_PASSWORD.getDisplayName())
                    .valid(false)
                    .explanation("a password must be provided for the given principal")
                    .build());
        }

        if (kerberosPasswordProvided && !kerberosPrincipalProvided) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_PRINCIPAL.getDisplayName())
                    .valid(false)
                    .explanation("a principal must be provided for the given password")
                    .build());
        }

        final KerberosCredentialsService kerberosCredentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

        if (kerberosCredentialsService != null && (kerberosPrincipalProvided || kerberosPasswordProvided)) {
            results.add(new ValidationResult.Builder()
                    .subject(KERBEROS_CREDENTIALS_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation("kerberos principal/password and kerberos credential service cannot be configured at the same time")
                    .build());
        }

        return results;
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

    @VisibleForTesting
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
                switch (colType) {
                    case BOOL:
                        row.addBoolean(columnIndex, DataTypeUtils.toBoolean(value, recordFieldName));
                        break;
                    case INT8:
                        row.addByte(columnIndex, DataTypeUtils.toByte(value, recordFieldName));
                        break;
                    case INT16:
                        row.addShort(columnIndex,  DataTypeUtils.toShort(value, recordFieldName));
                        break;
                    case INT32:
                        row.addInt(columnIndex,  DataTypeUtils.toInteger(value, recordFieldName));
                        break;
                    case INT64:
                        row.addLong(columnIndex,  DataTypeUtils.toLong(value, recordFieldName));
                        break;
                    case UNIXTIME_MICROS:
                        DataType fieldType = record.getSchema().getDataType(recordFieldName).get();
                        Timestamp timestamp = DataTypeUtils.toTimestamp(record.getValue(recordFieldName),
                                () -> DataTypeUtils.getDateFormat(fieldType.getFormat()), recordFieldName);
                        row.addTimestamp(columnIndex, timestamp);
                        break;
                    case STRING:
                        row.addString(columnIndex, DataTypeUtils.toString(value, recordFieldName));
                        break;
                    case BINARY:
                        row.addBinary(columnIndex, DataTypeUtils.toString(value, recordFieldName).getBytes());
                        break;
                    case FLOAT:
                        row.addFloat(columnIndex, DataTypeUtils.toFloat(value, recordFieldName));
                        break;
                    case DOUBLE:
                        row.addDouble(columnIndex, DataTypeUtils.toDouble(value, recordFieldName));
                        break;
                    case DECIMAL:
                        row.addDecimal(columnIndex, new BigDecimal(DataTypeUtils.toString(value, recordFieldName)));
                        break;
                    case VARCHAR:
                        row.addVarchar(columnIndex, DataTypeUtils.toString(value, recordFieldName));
                        break;
                    case DATE:
                        final Optional<DataType> fieldDataType = record.getSchema().getDataType(recordFieldName);
                        final String format = fieldDataType.isPresent() ? fieldDataType.get().getFormat() : RecordFieldType.DATE.getDefaultFormat();
                        row.addDate(columnIndex, getDate(value, recordFieldName, format));
                        break;
                    default:
                        throw new IllegalStateException(String.format("unknown column type %s", colType));
                }
            }
        }
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
        return DataTypeUtils.toDate(value, () -> getDateFormat(format), recordFieldName);
    }

    /**
     * Get Date Format using Date Record Field default pattern and system time zone to avoid unnecessary conversion
     *
     * @param format Date Format Pattern
     * @return Date Format used to parsing date fields
     */
    private DateFormat getDateFormat(final String format) {
        return new SimpleDateFormat(format);
    }

    /**
     * Converts a NiFi DataType to it's equivalent Kudu Type.
     */
    private Type toKuduType(DataType nifiType) {
        switch (nifiType.getFieldType()) {
            case BOOLEAN:
                return Type.BOOL;
            case BYTE:
                return Type.INT8;
            case SHORT:
                return Type.INT16;
            case INT:
                return Type.INT32;
            case LONG:
                return Type.INT64;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case DECIMAL:
                return Type.DECIMAL;
            case TIMESTAMP:
                return Type.UNIXTIME_MICROS;
            case CHAR:
            case STRING:
                return Type.STRING;
            case DATE:
                return Type.DATE;
            default:
                throw new IllegalArgumentException(String.format("unsupported type %s", nifiType));
        }
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
                .defaultValue(null)
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
}
