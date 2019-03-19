package org.apache.nifi.processors.kudu;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowError;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.client.Update;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.kudu.io.ResultHandler;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.record.Record;

import javax.security.auth.login.LoginException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractKuduProcessor extends AbstractProcessor {

    static final PropertyDescriptor KUDU_MASTERS = new Builder()
            .name("Kudu Masters")
            .description("Comma separated addresses of the Kudu masters to connect to.")
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

    static final PropertyDescriptor KUDU_OPERATION_TIMEOUT_MS = new Builder()
            .name("kudu-operations-timeout-ms")
            .displayName("Kudu Operation Timeout in MS")
            .description("Default timeout used for user operations (using sessions and scanners)")
            .required(false)
            .defaultValue(String.valueOf(AsyncKuduClient. DEFAULT_OPERATION_TIMEOUT_MS))
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor KUDU_SOCKET_OPERATION_TIMEOUT_MS = new Builder()
            .name("kudu-socket-operation-timeout-ms")
            .displayName("Kudu Socket Operation Timeout in MS")
            .description("Default timeout used for user operations")
            .required(false)
            .defaultValue(String.valueOf(AsyncKuduClient.DEFAULT_SOCKET_READ_TIMEOUT_MS))
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected KuduClient kuduClient;

    private AsyncKuduClient asyncKuduClient;

    private volatile KerberosUser kerberosUser;

    public KerberosUser getKerberosUser() {
        return this.kerberosUser;
    }

    public KuduClient getKuduClient() {
        return this.kuduClient;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        try {
            this.asyncKuduClient = createAsyncKuduClient(context);
        } catch (LoginException e){
            e.printStackTrace();
        }

        this.kuduClient = this.asyncKuduClient.syncClient();

        if(this.kuduClient != null) {
            try {
                this.kuduClient.getTablesList();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
        doOnTrigger(context, session);
    }

    private AsyncKuduClient createAsyncKuduClient(ProcessContext context) throws LoginException {
        final String kuduMasters = context.getProperty(KUDU_MASTERS).evaluateAttributeExpressions().getValue();
        final KerberosCredentialsService credentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

        if (credentialsService == null) {
            return buildClient(kuduMasters, context);
        }

        final String keytab = credentialsService.getKeytab();
        final String principal = credentialsService.getPrincipal();
        kerberosUser = loginKerberosUser(principal, keytab);

        final KerberosAction<AsyncKuduClient> kerberosAction = new KerberosAction<>(kerberosUser, () -> buildClient(kuduMasters, context), getLogger());
        return kerberosAction.execute();
    }


    protected AsyncKuduClient buildClient(final String masters, final ProcessContext context) {
        final String operationTimeout = context.getProperty(KUDU_OPERATION_TIMEOUT_MS).evaluateAttributeExpressions().getValue();
        final String adminOperationTimeout = context.getProperty(KUDU_SOCKET_OPERATION_TIMEOUT_MS).evaluateAttributeExpressions().getValue();

        return new AsyncKuduClient.AsyncKuduClientBuilder(masters)
                .defaultOperationTimeoutMs(Integer.parseInt(operationTimeout))
                .defaultSocketReadTimeoutMs(Integer.parseInt(adminOperationTimeout))
                .build();
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

    protected KerberosUser loginKerberosUser(final String principal, final String keytab) throws LoginException {
        final KerberosUser kerberosUser = new KerberosKeytabUser(principal, keytab);
        kerberosUser.login();
        return kerberosUser;
    }

    protected abstract void doOnTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException;

    @OnStopped
    public void shutdown() throws Exception {
        try {
            if (this.asyncKuduClient != null) {
                getLogger().debug("Closing KuduClient");
                this.asyncKuduClient.close();
                this.asyncKuduClient  = null;
            }
        } finally {
            if (kerberosUser != null) {
                kerberosUser.logout();
                kerberosUser = null;
            }
        }
    }

    protected void scan(ProcessContext context, ProcessSession session, KuduTable kuduTable, String predicates, List<String> projectedColumnNames, ResultHandler handler) throws Exception {
        AsyncKuduScanner.AsyncKuduScannerBuilder scannerBuilder = this.asyncKuduClient.newScannerBuilder(kuduTable);
        final String[] arrayPredicates = (predicates == null || predicates.isEmpty() ? new String[0] : predicates.split(","));

        for(String column : arrayPredicates){
            if (column.contains("=")) {
                final String[] parts = column.split("=");
                addPredicate(scannerBuilder, kuduTable, parts[0], parts[1], "EQUAL");
            } else if(column.contains(">")) {
                final String[] parts = column.split(">");
                addPredicate(scannerBuilder, kuduTable, parts[0], parts[1], "GREATER");
            } else if(column.contains("<")) {
                final String[] parts = column.split("<");
                addPredicate(scannerBuilder, kuduTable, parts[0], parts[1], "LESS");
            } else if(column.contains(">=")) {
                final String[] parts = column.split(">=");
                addPredicate(scannerBuilder, kuduTable, parts[0], parts[1], "GREATER_EQUAL");
            } else if(column.contains("<=")) {
                final String[] parts = column.split("<=");
                addPredicate(scannerBuilder, kuduTable, parts[0], parts[1], "LESS_EQUAL");
            }
        }

        if(!projectedColumnNames.isEmpty()){
            scannerBuilder.setProjectedColumnNames(projectedColumnNames);
        }

        AsyncKuduScanner scanner = scannerBuilder.build();
        while (scanner.hasMoreRows()) {
            handler.handle(scanner.nextRows().join());
        }
    }

    @VisibleForTesting
    protected void buildPartialRow(Schema schema, PartialRow row, Record record, List<String> fieldNames, Boolean ignoreNull) {

        for (String colName : fieldNames) {
            int colIdx = this.getColumnIndex(schema, colName);
            if (colIdx != -1) {
                ColumnSchema colSchema = schema.getColumnByIndex(colIdx);
                Type colType = colSchema.getType();

                if (record.getValue(colName) == null) {
                    if (schema.getColumnByIndex(colIdx).isKey()) {
                        throw new IllegalArgumentException(String.format("Can't set primary key column %s to null ", colName));
                    } else if(!schema.getColumnByIndex(colIdx).isNullable()) {
                        throw new IllegalArgumentException(String.format("Can't set primary key column %s to null ", colName));
                    }

                    if (!ignoreNull) {
                        row.setNull(colName);
                        continue;
                    }
                } else {
                    switch (colType.getDataType(colSchema.getTypeAttributes())) {
                        case BOOL:
                            row.addBoolean(colIdx, record.getAsBoolean(colName));
                            break;
                        case FLOAT:
                            row.addFloat(colIdx, record.getAsFloat(colName));
                            break;
                        case DOUBLE:
                            row.addDouble(colIdx, record.getAsDouble(colName));
                            break;
                        case BINARY:
                            row.addBinary(colIdx, record.getAsString(colName).getBytes());
                            break;
                        case INT8:
                            row.addByte(colIdx, record.getAsInt(colName).byteValue());
                            break;
                        case INT16:
                            row.addShort(colIdx, record.getAsInt(colName).shortValue());
                            break;
                        case INT32:
                            row.addInt(colIdx, record.getAsInt(colName));
                            break;
                        case INT64:
                        case UNIXTIME_MICROS:
                            row.addLong(colIdx, record.getAsLong(colName));
                            break;
                        case STRING:
                            row.addString(colIdx, record.getAsString(colName));
                            break;
                        case DECIMAL32:
                        case DECIMAL64:
                        case DECIMAL128:
                            row.addDecimal(colIdx, new BigDecimal(record.getAsString(colName)));
                            break;
                        default:
                            throw new IllegalStateException(String.format("unknown column type %s", colType));
                    }
                }
            }
        }
    }

    private void addPredicate(AsyncKuduScanner.AsyncKuduScannerBuilder scannerBuilder, KuduTable kuduTable, String column, String value, String comparisonOp) {
        ColumnSchema schema = kuduTable.getSchema().getColumn(column);
        Type colType = schema.getType();
        KuduPredicate predicate;
        switch (colType) {
            case STRING:
                if (value.isEmpty()) {
                    throw new IllegalStateException(String.format("give value is Empty for %s", column));
                }
                predicate = KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.valueOf(comparisonOp), value);
                break;
            case INT8:
                predicate = KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.valueOf(comparisonOp), Integer.valueOf(value));
                break;
            case INT16:
                predicate = KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.valueOf(comparisonOp), Integer.valueOf(value));
                break;
            case INT32:
                predicate = KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.valueOf(comparisonOp), Integer.valueOf(value));
                break;
            case INT64:
                predicate = KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.valueOf(comparisonOp), Long.valueOf(value));
                break;
            case BOOL:
                predicate = KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.valueOf(comparisonOp), Boolean.valueOf(value));
                break;
            case BINARY:
                predicate = KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.valueOf(comparisonOp), Byte.valueOf(value));
                break;
            case UNIXTIME_MICROS:
                predicate = KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.valueOf(comparisonOp), Integer.valueOf(value));
                break;
            case DOUBLE:
                predicate = KuduPredicate.newComparisonPredicate(schema, KuduPredicate.ComparisonOp.valueOf(comparisonOp), Double.valueOf(value));
                break;
            default:
                throw new IllegalStateException(String.format("unknown type %s", colType));
        }

        scannerBuilder.addPredicate(predicate);
    }

    private int getColumnIndex(Schema columns, String colName) {
        try {
            return columns.getColumnIndex(colName);
        } catch (Exception ex) {
            return -1;
        }
    }

    protected Upsert upsertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames, Boolean ignoreNull) {
        Upsert upsert = kuduTable.newUpsert();
        buildPartialRow(kuduTable.getSchema(), upsert.getRow(), record, fieldNames, ignoreNull);
        return upsert;
    }

    protected Insert insertRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames, Boolean ignoreNull) {
        Insert insert = kuduTable.newInsert();
        buildPartialRow(kuduTable.getSchema(), insert.getRow(), record, fieldNames, ignoreNull);
        return insert;
    }

    protected Delete deleteRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames, Boolean ignoreNull) {
        Delete delete = kuduTable.newDelete();
        buildPartialRow(kuduTable.getSchema(), delete.getRow(), record, fieldNames, ignoreNull);
        return delete;
    }

    protected Update updateRecordToKudu(KuduTable kuduTable, Record record, List<String> fieldNames, Boolean ignoreNull) {
        Update update = kuduTable.newUpdate();
        buildPartialRow(kuduTable.getSchema(), update.getRow(), record, fieldNames, ignoreNull);
        return update;
    }

    /**
     * Serializes a row from Kudu to a JSON document of the form:
     *
     * {
     *    "rows": [
     *      {
     *          "columnname-1" : "value1",
     *          "columnname-2" : "value2",
     *          "columnname-3" : "value3",
     *          "columnname-4" : "value4",
     *      },
     *      {
     *          "columnname-1" : "value1",
     *          "columnname-2" : "value2",
     *          "columnname-3" : "value3",
     *          "columnname-4" : "value4",
     *      }
     *    ]
     * }
     */
    protected String convertToJson(Iterator<RowResult> rows) {
        final StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{");

        jsonBuilder.append("\"rows\":[");
        while (rows.hasNext()) {
            RowResult result = rows.next();
            jsonBuilder.append("{");

            Iterator<ColumnSchema> columns = result.getSchema().getColumns().iterator();
            while (columns.hasNext()) {
                ColumnSchema col = columns.next();
                jsonBuilder.append("\"" + col.getName() + "\":");
                switch (col.getType()) {
                    case STRING:
                        jsonBuilder.append("\"" + result.getString(col.getName()) + "\"");
                        break;
                    case INT8:
                        jsonBuilder.append("\"" + result.getInt(col.getName()) + "\"");
                        break;
                    case INT16:
                        jsonBuilder.append("\"" + result.getInt(col.getName()) + "\"");
                        break;
                    case INT32:
                        jsonBuilder.append("\"" + result.getInt(col.getName()) + "\"");
                        break;
                    case INT64:
                        jsonBuilder.append("\"" + result.getLong(col.getName()) + "\"");
                        break;
                    case BOOL:
                        jsonBuilder.append("\"" + result.getBoolean(col.getName()) + "\"");
                        break;
                    case DECIMAL:
                        jsonBuilder.append("\"" + result.getDecimal(col.getName()) + "\"");
                        break;
                    case FLOAT:
                        jsonBuilder.append("\"" + result.getFloat(col.getName()) + "\"");
                        break;
                    case UNIXTIME_MICROS:
                        jsonBuilder.append("\"" + result.getLong(col.getName()) + "\"");
                        break;
                    case BINARY:
                        jsonBuilder.append("\"" + result.getBinary(col.getName()) + "\"");
                        break;
                    default:
                        break;
                }
                if(columns.hasNext())
                    jsonBuilder.append(",");
            }

            jsonBuilder.append("}");
            if (rows.hasNext()) {
                jsonBuilder.append(", ");
            }
        }
        // end row array
        jsonBuilder.append("]");

        // end overall document
        jsonBuilder.append("}");
        return jsonBuilder.toString();
    }
}