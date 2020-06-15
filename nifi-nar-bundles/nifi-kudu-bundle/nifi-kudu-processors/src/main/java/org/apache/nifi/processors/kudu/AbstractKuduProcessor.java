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
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Update;
import org.apache.kudu.client.Upsert;
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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
            .defaultValue(String.valueOf(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS) + "ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor KUDU_KEEP_ALIVE_PERIOD_TIMEOUT_MS = new Builder()
            .name("kudu-keep-alive-period-timeout-ms")
            .displayName("Kudu Keep Alive Period Timeout")
            .description("Default timeout used for user operations")
            .required(false)
            .defaultValue(String.valueOf(AsyncKuduClient.DEFAULT_KEEP_ALIVE_PERIOD_MS) + "ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
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
        final Integer operationTimeout = context.getProperty(KUDU_OPERATION_TIMEOUT_MS).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final Integer adminOperationTimeout = context.getProperty(KUDU_KEEP_ALIVE_PERIOD_TIMEOUT_MS).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS).intValue();

        return new KuduClient.KuduClientBuilder(masters)
                .defaultOperationTimeoutMs(operationTimeout)
                .defaultSocketReadTimeoutMs(adminOperationTimeout)
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
    protected void buildPartialRow(Schema schema, PartialRow row, Record record, List<String> fieldNames, Boolean ignoreNull, Boolean lowercaseFields) {
        for (String recordFieldName : fieldNames) {
            String colName = recordFieldName;
            if (lowercaseFields) {
                colName = colName.toLowerCase();
            }
            int colIdx = this.getColumnIndex(schema, colName);
            if (colIdx != -1) {
                ColumnSchema colSchema = schema.getColumnByIndex(colIdx);
                Type colType = colSchema.getType();

                if (record.getValue(recordFieldName) == null) {
                    if (schema.getColumnByIndex(colIdx).isKey()) {
                        throw new IllegalArgumentException(String.format("Can't set primary key column %s to null ", colName));
                    } else if(!schema.getColumnByIndex(colIdx).isNullable()) {
                        throw new IllegalArgumentException(String.format("Can't set column %s to null ", colName));
                    }

                    if (!ignoreNull) {
                        row.setNull(colName);
                    }
                } else {
                    Object value = record.getValue(recordFieldName);
                    switch (colType) {
                        case BOOL:
                            row.addBoolean(colIdx, DataTypeUtils.toBoolean(value, recordFieldName));
                            break;
                        case INT8:
                            row.addByte(colIdx, DataTypeUtils.toByte(value, recordFieldName));
                            break;
                        case INT16:
                            row.addShort(colIdx,  DataTypeUtils.toShort(value, recordFieldName));
                            break;
                        case INT32:
                            row.addInt(colIdx,  DataTypeUtils.toInteger(value, recordFieldName));
                            break;
                        case INT64:
                            row.addLong(colIdx,  DataTypeUtils.toLong(value, recordFieldName));
                            break;
                        case UNIXTIME_MICROS:
                            DataType fieldType = record.getSchema().getDataType(recordFieldName).get();
                            Timestamp timestamp = DataTypeUtils.toTimestamp(record.getValue(recordFieldName),
                                    () -> DataTypeUtils.getDateFormat(fieldType.getFormat()), recordFieldName);
                            row.addTimestamp(colIdx, timestamp);
                            break;
                        case STRING:
                            row.addString(colIdx, DataTypeUtils.toString(value, recordFieldName));
                            break;
                        case BINARY:
                            row.addBinary(colIdx, DataTypeUtils.toString(value, recordFieldName).getBytes());
                            break;
                        case FLOAT:
                            row.addFloat(colIdx, DataTypeUtils.toFloat(value, recordFieldName));
                            break;
                        case DOUBLE:
                            row.addDouble(colIdx, DataTypeUtils.toDouble(value, recordFieldName));
                            break;
                        case DECIMAL:
                            row.addDecimal(colIdx, new BigDecimal(DataTypeUtils.toString(value, recordFieldName)));
                            break;
                        default:
                            throw new IllegalStateException(String.format("unknown column type %s", colType));
                    }
                }
            }
        }
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

    private int getColumnIndex(Schema columns, String colName) {
        try {
            return columns.getColumnIndex(colName);
        } catch (Exception ex) {
            return -1;
        }
    }

    protected Upsert upsertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames, Boolean ignoreNull, Boolean lowercaseFields) {
        Upsert upsert = kuduTable.newUpsert();
        buildPartialRow(kuduTable.getSchema(), upsert.getRow(), record, fieldNames, ignoreNull, lowercaseFields);
        return upsert;
    }

    protected Insert insertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames, Boolean ignoreNull, Boolean lowercaseFields) {
        Insert insert = kuduTable.newInsert();
        buildPartialRow(kuduTable.getSchema(), insert.getRow(), record, fieldNames, ignoreNull, lowercaseFields);
        return insert;
    }

    protected Delete deleteRecordFromKudu(KuduTable kuduTable, Record record, List<String> fieldNames, Boolean ignoreNull, Boolean lowercaseFields) {
        Delete delete = kuduTable.newDelete();
        buildPartialRow(kuduTable.getSchema(), delete.getRow(), record, fieldNames, ignoreNull, lowercaseFields);
        return delete;
    }

    protected Update updateRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames, Boolean ignoreNull, Boolean lowercaseFields) {
        Update update = kuduTable.newUpdate();
        buildPartialRow(kuduTable.getSchema(), update.getRow(), record, fieldNames, ignoreNull, lowercaseFields);
        return update;
    }

}
