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
package org.apache.nifi.controller.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import javax.security.auth.login.LoginException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@CapabilityDescription("Lookup a record from Kudu Server associated with the specified key. Binary columns are base64 encoded")
@Tags({"lookup", "enrich", "key", "value", "kudu"})
public class KuduLookupService extends AbstractControllerService implements RecordLookupService {

    static final PropertyDescriptor KUDU_MASTERS = new PropertyDescriptor.Builder()
            .name("Kudu Masters")
            .description("Comma separated addresses of the Kudu masters to connect to.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials to use for authentication")
            .required(false)
            .identifiesControllerService(KerberosCredentialsService.class)
            .build();

    static final PropertyDescriptor KUDU_OPERATION_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("kudu-operations-timeout-ms")
            .displayName("Kudu Operation Timeout")
            .description("Default timeout used for user operations (using sessions and scanners)")
            .required(false)
            .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS + "ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor KUDU_KEEP_ALIVE_PERIOD_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("kudu-keep-alive-period-timeout-ms")
            .displayName("Kudu Keep Alive Period Timeout")
            .description("Default timeout used for user operations")
            .required(false)
            .defaultValue(AsyncKuduClient.DEFAULT_KEEP_ALIVE_PERIOD_MS + "ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("table-name")
            .displayName("Table Name")
            .description("Name of the table to access.")
            .required(true)
            .defaultValue("default")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor RETURN_COLUMNS = new PropertyDescriptor.Builder()
            .name("kudu-lu-return-cols")
            .displayName("Columns")
            .description("A comma-separated list of columns to return when scanning. To return all columns set to \"*\"")
            .required(true)
            .defaultValue("*")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();


    protected List<PropertyDescriptor> properties;

    protected KerberosCredentialsService credentialsService;
    private volatile KerberosUser kerberosUser;

    protected String kuduMasters;
    protected KuduClient kuduClient;
    protected volatile String tableName;
    protected volatile KuduTable table;
    protected volatile List<String> columnNames;

    protected volatile RecordSchema resultSchema;
    protected volatile Schema tableSchema;

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(KUDU_MASTERS);
        properties.add(KERBEROS_CREDENTIALS_SERVICE);
        properties.add(KUDU_OPERATION_TIMEOUT_MS);
        properties.add(KUDU_KEEP_ALIVE_PERIOD_TIMEOUT_MS);
        properties.add(TABLE_NAME);
        properties.add(RETURN_COLUMNS);
        addProperties(properties);
        this.properties = Collections.unmodifiableList(properties);
    }

    protected void addProperties(List<PropertyDescriptor> properties) {
    }

    protected void createKuduClient(ConfigurationContext context) throws LoginException {
        final String kuduMasters = context.getProperty(KUDU_MASTERS).evaluateAttributeExpressions().getValue();
        final KerberosCredentialsService credentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

        if (credentialsService == null) {
            return;
        }

        final String keytab = credentialsService.getKeytab();
        final String principal = credentialsService.getPrincipal();
        kerberosUser = loginKerberosUser(principal, keytab);

        final KerberosAction<KuduClient> kerberosAction = new KerberosAction<>(kerberosUser, () -> buildClient(kuduMasters, context), getLogger());
        this.kuduClient = kerberosAction.execute();
    }
    protected KerberosUser loginKerberosUser(final String principal, final String keytab) throws LoginException {
        final KerberosUser kerberosUser = new KerberosKeytabUser(principal, keytab);
        kerberosUser.login();
        return kerberosUser;
    }
    protected KuduClient buildClient(final String masters, final ConfigurationContext context) {
        final Integer operationTimeout = context.getProperty(KUDU_OPERATION_TIMEOUT_MS).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final Integer adminOperationTimeout = context.getProperty(KUDU_KEEP_ALIVE_PERIOD_TIMEOUT_MS).asTimePeriod(TimeUnit.MILLISECONDS).intValue();

        return new KuduClient.KuduClientBuilder(masters)
                .defaultOperationTimeoutMs(operationTimeout)
                .defaultSocketReadTimeoutMs(adminOperationTimeout)
                .build();
    }
    /**
     * Establish a connection to a Kudu cluster.
     * @param context the configuration context
     * @throws InitializationException if unable to connect a Kudu cluster
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {

        try {
            kuduMasters = context.getProperty(KUDU_MASTERS).evaluateAttributeExpressions().getValue();
            credentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

            if (kuduClient == null) {
                getLogger().debug("Setting up Kudu connection...");

                createKuduClient(context);
                getLogger().debug("Kudu connection successfully initialized");
            }
        } catch(Exception ex){
            getLogger().error("Exception occurred while interacting with Kudu due to " + ex.getMessage(), ex);
            throw new InitializationException(ex);
        }

        tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        try {
            table = kuduClient.openTable(tableName);
            tableSchema = table.getSchema();
            columnNames = getColumns(context.getProperty(RETURN_COLUMNS).getValue());

            //Result Schema
            resultSchema = kuduSchemaToNiFiSchema(tableSchema, columnNames);

        } catch (KuduException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Set<String> getRequiredKeys() {
        return new HashSet<>();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {

        //Scanner
        KuduScanner.KuduScannerBuilder builder = kuduClient.newScannerBuilder(table);

        builder.setProjectedColumnNames(columnNames);

        coordinates.forEach((key,value)->
                builder.addPredicate(createPredicate(tableSchema.getColumn(key), KuduPredicate.ComparisonOp.EQUAL, value))
        );

        KuduScanner kuduScanner = builder.build();

        //run lookup
        try {
            if (kuduScanner.hasMoreRows()) {
                RowResultIterator resultIterator = kuduScanner.nextRows();
                final Map<String, Object> values = new HashMap<>();

                //Only need first row
                if (resultIterator.hasNext()) {
                    RowResult result  = resultIterator.next();

                    for(String columnName : columnNames){
                        values.put(columnName,getObject(result,columnName));
                    }
                    return Optional.of(new MapRecord(resultSchema, values));
                }
            }
        } catch (KuduException ex) {
            throw new LookupFailureException("Failed to lookup from Kudu using these coordinates " + coordinates,ex);
        }
        //no match
        return Optional.empty();
    }

    private KuduPredicate createPredicate(ColumnSchema columnSchema,KuduPredicate.ComparisonOp comparisonOp, Object obj){
        if(obj instanceof Boolean)
            return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (Boolean)obj);
        else if(obj instanceof Double)
            return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (Double)obj);
        else if(obj instanceof Float)
            return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (Float)obj);
        else if(obj instanceof String)
            return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (String)obj);
        else if(obj instanceof BigDecimal)
            return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (BigDecimal)obj);
        else if(obj instanceof Long)
            return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (Long)obj);
        else if(obj instanceof Integer)
            return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (Integer)obj);
        else if(obj instanceof Timestamp)
            return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (Timestamp) obj);
        else if(obj instanceof byte[])
            return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (byte[]) obj);
        else if(obj instanceof Byte)
            return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (Byte) obj);
        else if(obj instanceof Short)
            return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (Short) obj);
        else
            throw new IllegalArgumentException("Unknown object type: "+obj.getClass().getCanonicalName());
    }
    private List<String> getColumns(String columns){
        if(columns.equals("*")){
            return tableSchema
                    .getColumns()
                    .stream().map(ColumnSchema::getName)
                    .collect(Collectors.toList());
        } else {
            return Arrays.asList(columns.split(","));
        }
    }
    private RecordSchema kuduSchemaToNiFiSchema(Schema kuduTableSchema, List<String> columnNames){
        final List<RecordField> fields = new ArrayList<>();
        for(String columnName : columnNames) {
            ColumnSchema cs = kuduTableSchema.getColumn(columnName);
            switch (cs.getType()) {
                case INT8:
                    fields.add(new RecordField(cs.getName(), RecordFieldType.BYTE.getDataType()));
                    break;
                case INT16:
                    fields.add(new RecordField(cs.getName(), RecordFieldType.SHORT.getDataType()));
                    break;
                case INT32:
                    fields.add(new RecordField(cs.getName(), RecordFieldType.INT.getDataType()));
                    break;
                case INT64:
                case UNIXTIME_MICROS:
                    fields.add(new RecordField(cs.getName(), RecordFieldType.LONG.getDataType()));
                    break;
                case BINARY:
                case STRING:
                case DECIMAL:
                    fields.add(new RecordField(cs.getName(), RecordFieldType.STRING.getDataType()));
                    break;
                case BOOL:
                    fields.add(new RecordField(cs.getName(), RecordFieldType.BOOLEAN.getDataType()));
                    break;
                case FLOAT:
                    fields.add(new RecordField(cs.getName(), RecordFieldType.FLOAT.getDataType()));
                    break;
                case DOUBLE:
                    fields.add(new RecordField(cs.getName(), RecordFieldType.DOUBLE.getDataType()));
                    break;
            }
        }
        return new SimpleRecordSchema(fields);
    }
    private Object getObject(RowResult result, String columnName) {
        Type type = result.getColumnType(columnName);
        switch (type) {
            case INT8:
                return result.getByte(columnName);
            case INT16:
                return result.getShort(columnName);
            case INT32:
                return result.getInt(columnName);
            case INT64:
            case UNIXTIME_MICROS:
                return result.getLong(columnName);
            case BINARY:
                return  Base64.getEncoder().encodeToString(result.getBinaryCopy(columnName));
            case STRING:
            case DECIMAL:
                return result.getString(columnName);
            case BOOL:
                return result.getBoolean(columnName);
            case FLOAT:
                return result.getFloat(columnName);
            case DOUBLE:
                return result.getDouble(columnName);
            default:
                throw new IllegalArgumentException("Unknown object type: "+type.getName());

        }
    }
    /**
     * Disconnect from the Kudu cluster.
     */
    @OnDisabled
    public void onDisabled() throws Exception {
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
}
