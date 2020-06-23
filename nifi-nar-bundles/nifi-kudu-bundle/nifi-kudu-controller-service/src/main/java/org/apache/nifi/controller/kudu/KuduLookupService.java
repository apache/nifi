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
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.ReplicaSelection;
import org.apache.kudu.client.RowResult;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosCredentialsService;
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


@CapabilityDescription("Lookup a record from Kudu Server associated with the specified key. Binary columns are base64 encoded. Only one matched row will be returned")
@Tags({"lookup", "enrich", "key", "value", "kudu"})
public class KuduLookupService extends AbstractControllerService implements RecordLookupService {

    public static final PropertyDescriptor KUDU_MASTERS = new PropertyDescriptor.Builder()
            .name("kudu-lu-masters")
            .displayName("Kudu Masters")
            .description("Comma separated addresses of the Kudu masters to connect to.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("kudu-lu-kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials to use for authentication")
            .required(false)
            .identifiesControllerService(KerberosCredentialsService.class)
            .build();

    public static final PropertyDescriptor KUDU_OPERATION_TIMEOUT_MS = new PropertyDescriptor.Builder()
            .name("kudu-lu-operations-timeout-ms")
            .displayName("Kudu Operation Timeout")
            .description("Default timeout used for user operations (using sessions and scanners)")
            .required(false)
            .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS + "ms")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final AllowableValue CLOSEST_REPLICA = new AllowableValue(ReplicaSelection.CLOSEST_REPLICA.toString(), ReplicaSelection.CLOSEST_REPLICA.name(),
            "Select the closest replica to the client. Replicas are classified from closest to furthest as follows: "+
                    "1) Local replicas 2) Replicas whose tablet server has the same location as the client 3) All other replicas");
    public static final AllowableValue LEADER_ONLY = new AllowableValue(ReplicaSelection.LEADER_ONLY.toString(), ReplicaSelection.LEADER_ONLY.name(),
            "Select the LEADER replica");
    public static final PropertyDescriptor KUDU_REPLICA_SELECTION = new PropertyDescriptor.Builder()
            .name("kudu-lu-replica-selection")
            .displayName("Kudu Replica Selection")
            .description("Policy with which to choose amongst multiple replicas")
            .required(true)
            .defaultValue(CLOSEST_REPLICA.getValue())
            .allowableValues(CLOSEST_REPLICA, LEADER_ONLY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("kudu-lu-table-name")
            .displayName("Kudu Table Name")
            .description("Name of the table to access.")
            .required(true)
            .defaultValue("default")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor RETURN_COLUMNS = new PropertyDescriptor.Builder()
            .name("kudu-lu-return-cols")
            .displayName("Kudu Return Columns")
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
    protected ReplicaSelection replicaSelection;
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
        properties.add(KUDU_REPLICA_SELECTION);
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

        if (credentialsService != null) {
            final String keytab = credentialsService.getKeytab();
            final String principal = credentialsService.getPrincipal();
            kerberosUser = loginKerberosUser(principal, keytab);

            final KerberosAction<KuduClient> kerberosAction = new KerberosAction<>(kerberosUser, () -> buildClient(kuduMasters, context), getLogger());
            this.kuduClient = kerberosAction.execute();
        } else {
            this.kuduClient = buildClient(kuduMasters, context);
        }
    }

    protected KerberosUser loginKerberosUser(final String principal, final String keytab) throws LoginException {
        final KerberosUser kerberosUser = new KerberosKeytabUser(principal, keytab);
        kerberosUser.login();
        return kerberosUser;
    }

    protected KuduClient buildClient(final String masters, final ConfigurationContext context) {
        final Integer operationTimeout = context.getProperty(KUDU_OPERATION_TIMEOUT_MS).asTimePeriod(TimeUnit.MILLISECONDS).intValue();

        return new KuduClient.KuduClientBuilder(masters)
                .defaultOperationTimeoutMs(operationTimeout)
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

        replicaSelection = ReplicaSelection.valueOf(context.getProperty(KUDU_REPLICA_SELECTION).getValue());
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
    public Optional<Record> lookup(Map<String, Object> coordinates) {

        //Scanner
        KuduScanner.KuduScannerBuilder builder = kuduClient.newScannerBuilder(table);

        builder.setProjectedColumnNames(columnNames);
        builder.replicaSelection(replicaSelection);

        //Only expecting one match
        builder.limit(1);

        coordinates.forEach((key,value)->
                builder.addPredicate(KuduPredicate.newComparisonPredicate(tableSchema.getColumn(key), KuduPredicate.ComparisonOp.EQUAL, value))
        );

        KuduScanner kuduScanner = builder.build();

        //Run lookup
        for ( RowResult row : kuduScanner){
            final Map<String, Object> values = new HashMap<>();
            for(String columnName : columnNames){
                Object object;
                if(row.getColumnType(columnName) == Type.BINARY){
                    object = Base64.getEncoder().encodeToString(row.getBinaryCopy(columnName));
                } else {
                    object = row.getObject(columnName);
                }
                values.put(columnName, object);
            }
            return Optional.of(new MapRecord(resultSchema, values));
        }

        //No match
        return Optional.empty();
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
            if(!kuduTableSchema.hasColumn(columnName)){
                throw new IllegalArgumentException("Column not found in Kudu table schema " + columnName);
            }
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
                    fields.add(new RecordField(cs.getName(), RecordFieldType.LONG.getDataType()));
                    break;
                case DECIMAL:
                    final ColumnTypeAttributes attributes = cs.getTypeAttributes();
                    fields.add(new RecordField(cs.getName(), RecordFieldType.DECIMAL.getDecimalDataType(attributes.getPrecision(), attributes.getScale())));
                    break;
                case UNIXTIME_MICROS:
                    fields.add(new RecordField(cs.getName(), RecordFieldType.TIMESTAMP.getDataType()));
                    break;
                case BINARY:
                case STRING:
                case VARCHAR:
                    fields.add(new RecordField(cs.getName(), RecordFieldType.STRING.getDataType()));
                    break;
                case DOUBLE:
                    fields.add(new RecordField(cs.getName(), RecordFieldType.DOUBLE.getDataType()));
                    break;
                case BOOL:
                    fields.add(new RecordField(cs.getName(), RecordFieldType.BOOLEAN.getDataType()));
                    break;
                case FLOAT:
                    fields.add(new RecordField(cs.getName(), RecordFieldType.FLOAT.getDataType()));
                    break;
                case DATE:
                    fields.add(new RecordField(cs.getName(), RecordFieldType.DATE.getDataType()));
                    break;
            }
        }
        return new SimpleRecordSchema(fields);
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
