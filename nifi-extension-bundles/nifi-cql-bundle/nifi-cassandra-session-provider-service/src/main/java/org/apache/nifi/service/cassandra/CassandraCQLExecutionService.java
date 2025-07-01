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
package org.apache.nifi.service.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.service.cassandra.mapping.FlexibleCounterCodec;
import org.apache.nifi.service.cassandra.mapping.JavaSQLTimestampCodec;
import org.apache.nifi.service.cql.api.CQLExecutionService;
import org.apache.nifi.service.cql.api.CQLFieldInfo;
import org.apache.nifi.service.cql.api.CQLQueryCallback;
import org.apache.nifi.service.cql.api.UpdateMethod;
import org.apache.nifi.service.cql.api.exception.QueryFailureException;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.oss.driver.api.core.type.DataTypes.ASCII;

@Tags({"cassandra", "dbcp", "database", "connection", "pooling"})
@CapabilityDescription("Provides connection session for Cassandra processors to work with Apache Cassandra.")
public class CassandraCQLExecutionService extends AbstractControllerService implements CQLExecutionService {

    public static final int DEFAULT_CASSANDRA_PORT = 9042;

    // Common descriptors

    private CqlSession cassandraSession;

    private Map<String, PreparedStatement> statementCache;

    private String keyspace;
    private int pageSize;

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONTACT_POINTS,
            CLIENT_AUTH,
            DATACENTER,
            KEYSPACE,
            USERNAME,
            PASSWORD,
            PROP_SSL_CONTEXT_SERVICE,
            FETCH_SIZE,
            READ_TIMEOUT,
            CONNECT_TIMEOUT,
            CONSISTENCY_LEVEL,
            COMPRESSION_TYPE
    );

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        connectToCassandra(context);
    }

    @OnDisabled
    public void onDisabled() {
        if (cassandraSession != null) {
            cassandraSession.close();
            cassandraSession = null;
        }
    }

    private void connectToCassandra(ConfigurationContext context) {
        if (cassandraSession == null) {
            this.pageSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions().asInteger();

            final String consistencyLevel = context.getProperty(CONSISTENCY_LEVEL).getValue();
            final String compression = context.getProperty(COMPRESSION_TYPE).getValue();
            final String contactPointList = context.getProperty(CONTACT_POINTS).evaluateAttributeExpressions().getValue();

            List<InetSocketAddress> contactPoints = getContactPoints(contactPointList);

            // Set up the client for secure (SSL/TLS communications) if configured to do so
            final SSLContextService sslService =
                    context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            final SSLContext sslContext;

            if (sslService == null) {
                sslContext = null;
            } else {
                sslContext = sslService.createContext();
            }

            final String username, password;
            PropertyValue usernameProperty = context.getProperty(USERNAME).evaluateAttributeExpressions();
            PropertyValue passwordProperty = context.getProperty(PASSWORD).evaluateAttributeExpressions();

            if (usernameProperty != null && passwordProperty != null) {
                username = usernameProperty.getValue();
                password = passwordProperty.getValue();
            } else {
                username = null;
                password = null;
            }

            final Duration readTimeout = context.getProperty(READ_TIMEOUT).evaluateAttributeExpressions().asDuration();
            final Duration connectTimeout = context.getProperty(CONNECT_TIMEOUT).evaluateAttributeExpressions().asDuration();

            final String datacenter = context.getProperty(DATACENTER).evaluateAttributeExpressions().getValue();

            keyspace = context.getProperty(KEYSPACE).isSet() ? context.getProperty(KEYSPACE).evaluateAttributeExpressions().getValue() : null;

            DriverConfigLoader loader =
                    DriverConfigLoader.programmaticBuilder()
                            .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, connectTimeout)
                            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, readTimeout)
                            .withString(DefaultDriverOption.REQUEST_CONSISTENCY, consistencyLevel)
                            .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, compression)
                            .build();

            CqlSessionBuilder builder = CqlSession.builder()
                    .addContactPoints(contactPoints);
            if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
                    builder = builder.withAuthCredentials(username, password);
            }

            final CqlSession cqlSession = builder
                    .withSslContext(sslContext)
                    .withLocalDatacenter(datacenter)
                    .withKeyspace(keyspace)
                    .withConfigLoader(loader)
                    .build();

            MutableCodecRegistry codecRegistry =
                    (MutableCodecRegistry) cqlSession.getContext().getCodecRegistry();

            codecRegistry.register(new JavaSQLTimestampCodec());
            codecRegistry.register(new FlexibleCounterCodec());

            // Create the cluster and connect to it
            cassandraSession = cqlSession;
        }
    }

    private List<InetSocketAddress> getContactPoints(String contactPointList) {

        if (contactPointList == null) {
            return null;
        }

        final String[] contactPointStringList = contactPointList.split(",");
        List<InetSocketAddress> contactPoints = new ArrayList<>();

        for (String contactPointEntry : contactPointStringList) {
            String[] addresses = contactPointEntry.split(":");
            final String hostName = addresses[0].trim();
            final int port = (addresses.length > 1) ? Integer.parseInt(addresses[1].trim()) : DEFAULT_CASSANDRA_PORT;

            contactPoints.add(new InetSocketAddress(hostName, port));
        }

        return contactPoints;
    }

    @Override
    public void query(String cql, boolean cacheStatement, List parameters, CQLQueryCallback callback) throws QueryFailureException {
        SimpleStatement statement = SimpleStatement.builder(cql)
                .setPageSize(pageSize).build();
        PreparedStatement preparedStatement = cassandraSession.prepare(statement);


        //TODO: cache
        BoundStatement boundStatement = parameters != null && !parameters.isEmpty()
                ? preparedStatement.bind(parameters.toArray()) : preparedStatement.bind();
        ResultSet results = cassandraSession.execute(boundStatement);

        Iterator<Row> resultsIterator = results.iterator();
        long rowNumber = 0;

        List<CQLFieldInfo> columnDefinitions = new ArrayList<>();
        AtomicReference<RecordSchema> schemaReference = new AtomicReference<>();

        try {
            while (resultsIterator.hasNext()) {
                try {
                    Row row = resultsIterator.next();

                    if (schemaReference.get() == null) {
                        Schema generatedAvroSchema = createSchema(results);
                        RecordSchema converted = AvroTypeUtil.createSchema(generatedAvroSchema);
                        schemaReference.set(converted);
                    }

                    if (columnDefinitions.isEmpty()) {
                        row.getColumnDefinitions().forEach(def -> {
                            CQLFieldInfo info = new CQLFieldInfo(def.getName().toString(),
                                    def.getType().toString(), def.getType().getProtocolCode());
                            columnDefinitions.add(info);
                        });
                    }

                    Map<String, Object> resultMap = new HashMap<>();

                    for (int x = 0; x < columnDefinitions.size(); x++) {
                        resultMap.put(columnDefinitions.get(x).getFieldName(), row.getObject(x));
                    }

                    MapRecord record = new MapRecord(schemaReference.get(), resultMap);

                    callback.receive(++rowNumber, record, columnDefinitions, resultsIterator.hasNext());
                } catch (Exception ex) {
                    throw new ProcessException("Error querying CQL", ex);
                }
            }
        } catch (QueryExecutionException qee) {
            getLogger().error("Error executing query", qee);
            throw new QueryFailureException();
        }
    }

    protected GeneratedResult generateInsert(String cassandraTable, RecordSchema schema, Map<String, Object> recordContentMap) {
        InsertInto insertQuery;
        List<String> keys = new ArrayList<>();

        if (cassandraTable.contains(".")) {
            String[] keyspaceAndTable = cassandraTable.split("\\.");
            insertQuery = QueryBuilder.insertInto(keyspaceAndTable[0], keyspaceAndTable[1]);
        } else {
            insertQuery = QueryBuilder.insertInto(cassandraTable);
        }

        RegularInsert regularInsert = null;
        for (String fieldName : schema.getFieldNames()) {
            Object value = recordContentMap.get(fieldName);

            if (value != null && value.getClass().isArray()) {
                Object[] array = (Object[]) value;

                if (array.length > 0) {
                    if (array[0] instanceof Byte) {
                        Object[] temp = (Object[]) value;
                        byte[] newArray = new byte[temp.length];
                        for (int x = 0; x < temp.length; x++) {
                            newArray[x] = (Byte) temp[x];
                        }
                        value = ByteBuffer.wrap(newArray);
                    }
                }
            }

            if (schema.getDataType(fieldName).isPresent()) {
                org.apache.nifi.serialization.record.DataType fieldDataType = schema.getDataType(fieldName).get();
                if (fieldDataType.getFieldType() == RecordFieldType.ARRAY) {
                    if (((ArrayDataType) fieldDataType).getElementType().getFieldType() == RecordFieldType.STRING) {
                        value = Arrays.stream((Object[]) value).toArray(String[]::new);
                    }
                }
            }

            if (regularInsert == null) {
                regularInsert = insertQuery.value(fieldName, QueryBuilder.bindMarker(fieldName));
            } else {
                regularInsert = regularInsert.value(fieldName, QueryBuilder.bindMarker(fieldName));
            }

            keys.add(fieldName);
        }

        if (regularInsert == null) {
            throw new ProcessException("Could not build an insert statement from the supplied record");
        }

        return new GeneratedResult(regularInsert.build(), keys);
    }

    @Override
    public void insert(String table, org.apache.nifi.serialization.record.Record record) {
        GeneratedResult result = generateInsert(table, record.getSchema(), ((MapRecord) record).toMap(true));
        PreparedStatement preparedStatement = cassandraSession.prepare(result.statement);

        BoundStatement boundStatement = preparedStatement.bind(getBindValues(record, result.keysUsed));

        cassandraSession.execute(boundStatement);
    }

    @Override
    public void insert(String table, List<org.apache.nifi.serialization.record.Record> records) {
        if (records == null || records.isEmpty()) {
            return;
        }

        BatchStatementBuilder builder = BatchStatement.builder(BatchType.LOGGED);
        GeneratedResult result = generateInsert(table, records.get(0).getSchema(), ((MapRecord) records.get(0)).toMap(true));
        PreparedStatement preparedStatement = cassandraSession.prepare(result.statement);

        for (org.apache.nifi.serialization.record.Record record : records) {
            builder.addStatement(preparedStatement.bind(getBindValues(record, result.keysUsed)));
        }
        cassandraSession.execute(builder.build());
    }

    @Override
    public String getTransitUrl(String tableName) {
        return  "cassandra://" + cassandraSession.getMetadata().getClusterName() + "." + tableName;
    }

    /**
     * Creates an Avro schema from the given result set. The metadata (column definitions, data types, etc.) is used
     * to determine a schema for Avro.
     *
     * @param rs The result set from which an Avro schema will be created
     * @return An Avro schema corresponding to the given result set's metadata
     */
    public static Schema createSchema(final ResultSet rs) {
        final ColumnDefinitions columnDefinitions = rs.getColumnDefinitions();
        final int nrOfColumns = (columnDefinitions == null ? 0 : columnDefinitions.size());
        String tableName = "NiFi_Cassandra_Query_Record";
        if (nrOfColumns > 0) {
            String tableNameFromMeta = columnDefinitions.get(0).getTable().toString(); //.getTable(0);
            if (!StringUtils.isBlank(tableNameFromMeta)) {
                tableName = tableNameFromMeta;
            }
        }

        final SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record(tableName).namespace("any.data").fields();
        if (columnDefinitions != null) {
            for (int i = 0; i < nrOfColumns; i++) {

                DataType dataType = columnDefinitions.get(i).getType();
                if (dataType == null) {
                    throw new IllegalArgumentException("No data type for column[" + i + "] with name "
                            + columnDefinitions.get(i).getName());
                }

                if (dataType instanceof ListType l) {
                    builder.name(columnDefinitions.get(i).getName().toString()).type().unionOf().nullBuilder().endNull().and().array()
                            .items(getUnionFieldType(getPrimitiveAvroTypeFromCassandraType(l.getElementType()))).endUnion().noDefault();
                } else if (dataType instanceof SetType s) {
                    builder.name(columnDefinitions.get(i).getName().toString()).type().unionOf().nullBuilder().endNull().and().array()
                            .items(getUnionFieldType(getPrimitiveAvroTypeFromCassandraType(s.getElementType()))).endUnion().noDefault();
                } else if (dataType instanceof MapType m) {
                    builder.name(columnDefinitions.get(i).getName().toString()).type().unionOf().nullBuilder().endNull().and().map().values(
                            getUnionFieldType(getPrimitiveAvroTypeFromCassandraType(m.getValueType()))).endUnion().noDefault();
                } else {
                    builder.name(columnDefinitions.get(i).getName().toString())
                            .type(getUnionFieldType(getPrimitiveAvroTypeFromCassandraType(dataType))).noDefault();
                }
            }
        }
        return builder.endRecord();
    }

    /**
     * This method will create a schema a union field consisting of null and the specified type.
     *
     * @param dataType The data type of the field
     */
    protected static Schema getUnionFieldType(String dataType) {
        return SchemaBuilder.builder().unionOf().nullBuilder().endNull().and().type(getSchemaForType(dataType)).endUnion();
    }

    /**
     * This method will create an Avro schema for the specified type.
     *
     * @param dataType The data type of the field
     */
    protected static Schema getSchemaForType(final String dataType) {
        final SchemaBuilder.TypeBuilder<Schema> typeBuilder = SchemaBuilder.builder();
        final Schema returnSchema = switch (dataType) {
            case "string" -> typeBuilder.stringType();
            case "boolean" -> typeBuilder.booleanType();
            case "int" -> typeBuilder.intType();
            case "long" -> typeBuilder.longType();
            case "float" -> typeBuilder.floatType();
            case "double" -> typeBuilder.doubleType();
            case "bytes" -> typeBuilder.bytesType();
            default -> throw new IllegalArgumentException("Unknown Avro primitive type: " + dataType);
        };
        return returnSchema;
    }

    protected static String getPrimitiveAvroTypeFromCassandraType(final DataType dataType) {
        // Map types from Cassandra to Avro where possible
        if (dataType.equals(ASCII)
                || dataType.equals(DataTypes.TEXT)
                // Nonstandard types represented by this processor as a string
                || dataType.equals(DataTypes.TIMESTAMP)
                || dataType.equals(DataTypes.TIMEUUID)
                || dataType.equals(DataTypes.UUID)
                || dataType.equals(DataTypes.INET)
                || dataType.equals(DataTypes.VARINT)) {
            return "string";

        } else if (dataType.equals(DataTypes.BOOLEAN)) {
            return "boolean";

        } else if (dataType.equals(DataTypes.INT)) {
            return "int";

        } else if (dataType.equals(DataTypes.BIGINT)
                || dataType.equals(DataTypes.COUNTER)) {
            return "long";

        } else if (dataType.equals(DataTypes.FLOAT)) {
            return "float";

        } else if (dataType.equals(DataTypes.DOUBLE)) {
            return "double";

        } else if (dataType.equals(DataTypes.BLOB)) {
            return "bytes";

        } else {
            throw new IllegalArgumentException("createSchema: Unknown Cassandra data type " + dataType
                    + " cannot be converted to Avro type");
        }
    }

    private String[] getTableAndKeyspace(String cassandraTable) {
        if (cassandraTable.contains(".")) {
            return cassandraTable.split("\\.");
        } else {
            return new String[] {keyspace, cassandraTable};
        }
    }

    protected SimpleStatement generateDelete(String cassandraTable, org.apache.nifi.serialization.record.Record record, List<String> deleteKeyNames) {
        DeleteSelection deleteSelection;
        RecordSchema schema = record.getSchema();

        // Split up the update key names separated by a comma, should not be empty
        if (deleteKeyNames == null || deleteKeyNames.isEmpty()) {
            throw new IllegalArgumentException("No delete keys were specified");
        }

        // Verify if all update keys are present in the record
        for (String deleteKey : deleteKeyNames) {
            if (!schema.getFieldNames().contains(deleteKey)) {
                throw new IllegalArgumentException("Delete key '" + deleteKey + "' is not present in the record schema");
            }
        }

        final String[] tableAndKeyspace = getTableAndKeyspace(cassandraTable);

        deleteSelection = QueryBuilder.deleteFrom(tableAndKeyspace[0], tableAndKeyspace[1]);

        List<String> otherKeys = schema.getFieldNames().stream()
                .filter(fieldName -> !deleteKeyNames.contains(fieldName))
                .toList();

        List<Relation> whereCriteria = new ArrayList<>();

        for (String fieldName : otherKeys) {
            whereCriteria.add(Relation.column("k").isEqualTo(QueryBuilder.bindMarker(fieldName)));
        }

        return deleteSelection.where(whereCriteria).build();
    }

    protected GeneratedResult generateUpdate(String cassandraTable, org.apache.nifi.serialization.record.Record record, List<String> updateKeyNames, UpdateMethod updateMethod) {
        UpdateStart updateQueryStart;
        RecordSchema schema = record.getSchema();

        List<String> keysUsedInOrder = new ArrayList<>();

        // Split up the update key names separated by a comma, should not be empty
        if (updateKeyNames == null || updateKeyNames.isEmpty()) {
            throw new IllegalArgumentException("No Update Keys were specified");
        }

        // Verify if all update keys are present in the record
        for (String updateKey : updateKeyNames) {
            if (!schema.getFieldNames().contains(updateKey)) {
                throw new IllegalArgumentException("Update key '" + updateKey + "' is not present in the record schema");
            }
        }

        // Prepare keyspace/table names
        if (cassandraTable.contains(".")) {
            String[] keyspaceAndTable = cassandraTable.split("\\.");
            updateQueryStart = QueryBuilder.update(keyspaceAndTable[0], keyspaceAndTable[1]);
        } else {
            updateQueryStart = QueryBuilder.update(cassandraTable);
        }

        UpdateWithAssignments updateAssignments = null;

        List<String> otherKeys = schema.getFieldNames().stream()
                .filter(fieldName -> !updateKeyNames.contains(fieldName))
                .toList();

        for (String fieldName : otherKeys) {
            if (updateMethod == UpdateMethod.SET) {
                updateAssignments = updateAssignments == null ? updateQueryStart.setColumn(fieldName, QueryBuilder.bindMarker(fieldName))
                        : updateAssignments.setColumn(fieldName, QueryBuilder.bindMarker(fieldName));
            } else if (updateMethod == UpdateMethod.INCREMENT) {
                updateAssignments = updateAssignments == null ? updateQueryStart.increment(fieldName, QueryBuilder.bindMarker(fieldName))
                        : updateAssignments.increment(fieldName, QueryBuilder.bindMarker(fieldName));
            } else if (updateMethod == UpdateMethod.DECREMENT) {
                updateAssignments = updateAssignments == null ? updateQueryStart.decrement(fieldName, QueryBuilder.bindMarker(fieldName))
                        : updateAssignments.decrement(fieldName, QueryBuilder.bindMarker(fieldName));
            } else {
                throw new IllegalArgumentException("Update Method '" + updateMethod + "' is not valid.");
            }

            keysUsedInOrder.add(fieldName);
        }

        if (updateAssignments == null) {
            throw new ProcessException("No update assignment found");
        }

        Update update = null;

        for (String fieldName : updateKeyNames) {
            update = update == null ? updateAssignments.whereColumn(fieldName).isEqualTo(QueryBuilder.bindMarker(fieldName))
                    : update.whereColumn(fieldName).isEqualTo(QueryBuilder.bindMarker(fieldName));
            keysUsedInOrder.add(fieldName);
        }

        return new GeneratedResult(update.build(),  keysUsedInOrder);
    }

    @Override
    public void delete(String cassandraTable, org.apache.nifi.serialization.record.Record record, List<String> updateKeys) {
        Statement deleteStatement = generateDelete(cassandraTable, record, updateKeys);
        cassandraSession.execute(deleteStatement);
    }

    @Override
    public void update(String cassandraTable, org.apache.nifi.serialization.record.Record record, List<String> updateKeys, UpdateMethod updateMethod) {
        GeneratedResult result = generateUpdate(cassandraTable, record, updateKeys, updateMethod);
        PreparedStatement preparedStatement = cassandraSession.prepare(result.statement());

        BoundStatement statement = preparedStatement.bind(getBindValues(record, result.keysUsed()));

        cassandraSession.execute(statement);
    }

    private Object[] getBindValues(org.apache.nifi.serialization.record.Record  record, List<String> keyNamesInOrder) {
        Object[] result = new Object[keyNamesInOrder.size()];

        for (int i = 0; i < keyNamesInOrder.size(); i++) {
            result[i] = record.getValue(keyNamesInOrder.get(i));
        }

        return result;
    }

    @Override
    public void update(String cassandraTable, List<org.apache.nifi.serialization.record.Record> records, List<String> updateKeys, UpdateMethod updateMethod) {
        if (records == null || records.isEmpty()) {
            return;
        }

        BatchStatementBuilder builder = BatchStatement.builder(BatchType.LOGGED);

        GeneratedResult result = generateUpdate(cassandraTable, records.get(0), updateKeys, updateMethod);
        PreparedStatement preparedStatement = cassandraSession.prepare(result.statement());

        for (org.apache.nifi.serialization.record.Record record : records) {
            builder.addStatement(preparedStatement.bind(getBindValues(record, result.keysUsed())));
        }
        cassandraSession.execute(builder.build());
    }

    record GeneratedResult(SimpleStatement statement, List<String> keysUsed) {

    }
}
