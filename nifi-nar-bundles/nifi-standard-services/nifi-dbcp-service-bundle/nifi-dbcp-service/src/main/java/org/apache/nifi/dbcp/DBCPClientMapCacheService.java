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
package org.apache.nifi.dbcp;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.AtomicCacheEntry;
import org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.List;

@Tags({"distributed", "cache", "state", "map", "cluster", "rdbms", "database"})
@CapabilityDescription("A relational-database-based map cache service. The processor expects a primary key on the key-column " +
        "and should work with most ANSI SQL Databases like Derby, PostgreSQL, MySQL, and MariaDB. The key and value columns " +
        "are expected to be varbinary and the revision column should be a long")
public class DBCPClientMapCacheService extends AbstractControllerService implements AtomicDistributedMapCacheClient<Long> {

    public static final PropertyDescriptor DBCP_CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("dbcp-connection-pool")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor SCHEMA_NAME = new PropertyDescriptor.Builder()
            .name("schema-name")
            .displayName("Schema Name")
            .description("The name of the database schema to be queried. Note that this may be case-sensitive depending on the database.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("table-name")
            .displayName("Table Name")
            .description("The name of the database table to be queried. Note that this may be case-sensitive depending on the database.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_COLUMN_NAME = new PropertyDescriptor.Builder()
            .name("key-column")
            .displayName("Key Column")
            .description("The primary key column in the table being queried. Note that this may be case-sensitive depending on the database.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor VAL_COLUMN_NAME = new PropertyDescriptor.Builder()
            .name("value-column")
            .displayName("Value Column")
            .description("The value column in the table being queried. Note that this may be case-sensitive depending on the database.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REV_COLUMN_NAME = new PropertyDescriptor.Builder()
            .name("revision-column")
            .displayName("Revision Column")
            .description("The revision column in the table being queried. Note that this may be case-sensitive depending on the database.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DBCP_CONNECTION_POOL);
        descriptors.add(SCHEMA_NAME);
        descriptors.add(TABLE_NAME);
        descriptors.add(KEY_COLUMN_NAME);
        descriptors.add(VAL_COLUMN_NAME);
        descriptors.add(REV_COLUMN_NAME);
        return descriptors;
    }

    private DBCPService dbcpConnectionPool;
    private String schemaName;
    private String tableName;
    private String keyColName;
    private String valColName;
    private String revColName;

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        dbcpConnectionPool = context.getProperty(DBCP_CONNECTION_POOL).asControllerService(DBCPService.class);
        schemaName = context.getProperty(SCHEMA_NAME).evaluateAttributeExpressions().getValue();
        tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        keyColName = context.getProperty(KEY_COLUMN_NAME).evaluateAttributeExpressions().getValue();
        valColName = context.getProperty(VAL_COLUMN_NAME).evaluateAttributeExpressions().getValue();
        revColName = context.getProperty(REV_COLUMN_NAME).evaluateAttributeExpressions().getValue();

        try (Connection con = dbcpConnectionPool.getConnection()) {
            DatabaseMetaData dmd = con.getMetaData();
            try (ResultSet rs = dmd.getPrimaryKeys(null, schemaName, tableName)) {
                if (!rs.next()) {
                    throw new InitializationException("No Primary Key Defined");
                }
            }
        } catch (SQLException e) {
            throw new InitializationException(e);
        }
    }

    private String buildSelect() {
        if (schemaName != null && !schemaName.isEmpty()) {
            return String.format("select \"%s\" from \"%s\".\"%s\" where \"%s\" = ?", valColName, schemaName, tableName, keyColName);
        } else {
            return String.format("select \"%s\" from \"%s\" where \"%s\" = ?", valColName, tableName, keyColName);
        }
    }

    private String buildAtomicSelect() {
        if (schemaName != null && !schemaName.isEmpty()) {
            return String.format("select \"%s\", \"%s\" from \"%s\".\"%s\" where \"%s\" = ?", valColName, revColName, schemaName, tableName, keyColName);
        } else {
            return String.format("select \"%s\", \"%s\" from \"%s\" where \"%s\" = ?", valColName, revColName, tableName, keyColName);
        }
    }

    private String buildInsert() {
        if (schemaName != null && !schemaName.isEmpty()) {
            return String.format("insert into \"%s\".\"%s\"(\"%s\", \"%s\") values(?, ?)", schemaName, tableName, keyColName, valColName);
        } else {
            return String.format("insert into \"%s\"(\"%s\", \"%s\") values(?, ?)", tableName, keyColName, valColName);
        }
    }

    private String buildAtomicInsert() {
        if (schemaName != null && !schemaName.isEmpty()) {
            return String.format("insert into \"%s\".\"%s\"(\"%s\", \"%s\", \"%s\") values(?, ?, ?)", schemaName, tableName, keyColName, valColName, revColName);
        } else {
            return String.format("insert into \"%s\"(\"%s\", \"%s\", \"%s\") values(?, ?, ?)", tableName, keyColName, valColName, revColName);
        }
    }

    private String buildUpdate() {
        if (schemaName != null && !schemaName.isEmpty()) {
            return String.format("update \"%s\".\"%s\" set \"%s\" = ? where \"%s\" = ?", schemaName, tableName, valColName, keyColName);
        } else {
            return String.format("update \"%s\" set \"%s\" = ? where \"%s\" = ?", tableName, valColName, keyColName);
        }
    }

    private String buildAtomicUpdate() {
        if (schemaName != null && !schemaName.isEmpty()) {
            return String.format("update \"%s\".\"%s\" set \"%s\" = ?, \"%s\" = ? where \"%s\" = ? and \"%s\" = ?", schemaName, tableName, valColName, revColName, keyColName, revColName);
        } else {
            return String.format("update \"%s\" set \"%s\" = ?, \"%s\" = ? where \"%s\" = ? and \"%s\" = ?", tableName, valColName, revColName, keyColName, revColName);
        }
    }

    private String buildDelete() {
        if (schemaName != null && !schemaName.isEmpty()) {
            return String.format("delete from \"%s\".\"%s\" where \"%s\" = ?", schemaName, tableName, keyColName);
        } else {
            return String.format("delete from \"%s\" where \"%s\" = ?", tableName, keyColName);
        }
    }

    private <T> byte[] serialize(final T value, final Serializer<T> serializer) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializer.serialize(value, baos);
        return baos.toByteArray();
    }

    private <T> T deserialize(final byte[] value, final Deserializer<T> deserializer) throws IOException {
        return deserializer.deserialize(value);
    }

    @Override
    public <K, V> AtomicCacheEntry<K, V, Long> fetch(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        final byte[] keyBytes = serialize(key, keySerializer);
        try (Connection con = dbcpConnectionPool.getConnection()) {
            try (PreparedStatement stmt = con.prepareStatement(buildAtomicSelect())) {
                stmt.setBytes(1, keyBytes);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        byte[] valueBytes = rs.getBytes(1);
                        Long revLong = rs.getLong(2);
                        return new AtomicCacheEntry<>(key, deserialize(valueBytes, valueDeserializer), revLong);
                    }
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to Execute " + buildDelete(), e);
        }
        return null;
    }

    @Override
    public <K, V> boolean replace(AtomicCacheEntry<K, V, Long> entry, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final byte[] keyBytes = serialize(entry.getKey(), keySerializer);
        final byte[] valueBytes = serialize(entry.getValue(), valueSerializer);
        final Long revLong = entry.getRevision().orElse(1L);
        try (Connection con = dbcpConnectionPool.getConnection()) {
            try (PreparedStatement stmt = con.prepareStatement(buildAtomicInsert())) {
                stmt.setBytes(1, keyBytes);
                stmt.setBytes(2, valueBytes);
                stmt.setLong(3, revLong);
                stmt.executeUpdate();
            } catch (SQLIntegrityConstraintViolationException e) {
                try (PreparedStatement stmt = con.prepareStatement(buildAtomicUpdate())) {
                    stmt.setBytes(1, valueBytes);
                    stmt.setLong(2, revLong + 1);
                    stmt.setBytes(3, keyBytes);
                    stmt.setLong(4, revLong);
                    int rowCount = stmt.executeUpdate();
                    if (rowCount == 0) return false;
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to Execute " + buildAtomicUpdate(), e);
        }
        return true;
    }

    @Override
    public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final byte[] keyBytes = serialize(key, keySerializer);
        final byte[] valueBytes = serialize(value, valueSerializer);
        try (Connection con = dbcpConnectionPool.getConnection()) {
            try (PreparedStatement stmt = con.prepareStatement(buildInsert())) {
                stmt.setBytes(1, keyBytes);
                stmt.setBytes(2, valueBytes);
                int rowCount = stmt.executeUpdate();
                if (rowCount == 0) {
                    throw new IOException("Failed to Insert Value");
                }
            }
        } catch (SQLException e) {
            String sqlState = e.getSQLState();
            if (sqlState.startsWith("23")) {
                return false;
            } else {
                throw new IOException("Failed to Execute " + buildInsert(), e);
            }
        }
        return true;
    }

    /**
     * This logic was copied from the HBase Processors. This can probably be made
     * atomic but will require database specific logic to implement.
     **/
    @Override
    public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) throws IOException {
        final V got = get(key, keySerializer, valueDeserializer);
        final boolean wasAbsent = putIfAbsent(key, value, keySerializer, valueSerializer);

        if (!wasAbsent) return got;
        else return null;
    }

    @Override
    public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
        final byte[] keyBytes = serialize(key, keySerializer);
        try (Connection con = dbcpConnectionPool.getConnection()) {
            try (PreparedStatement stmt = con.prepareStatement(buildSelect())) {
                stmt.setBytes(1, keyBytes);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return true;
                    }
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to Execute " + buildSelect(), e);
        }
        return false;
    }

    @Override
    public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final byte[] keyBytes = serialize(key, keySerializer);
        final byte[] valueBytes = serialize(value, valueSerializer);
        if (!putIfAbsent(key, value, keySerializer, valueSerializer)) {
            try (Connection con = dbcpConnectionPool.getConnection()) {
                try (PreparedStatement stmt = con.prepareStatement(buildUpdate())) {
                    stmt.setBytes(1, valueBytes);
                    stmt.setBytes(2, keyBytes);
                    int rowCount = stmt.executeUpdate();
                    if (rowCount == 0) {
                        throw new IOException("Failed to Update Value");
                    }
                }
            } catch (SQLException e) {
                throw new IOException("Failed to Execute " + buildUpdate(), e);
            }
        }
    }

    @Override
    public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        final byte[] keyBytes = serialize(key, keySerializer);
        final byte[] valueBytes;
        try (Connection con = dbcpConnectionPool.getConnection()) {
            try (PreparedStatement stmt = con.prepareStatement(buildSelect())) {
                stmt.setBytes(1, keyBytes);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        valueBytes = rs.getBytes(1);
                        if (valueBytes != null) {
                            return deserialize(valueBytes, valueDeserializer);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to Execute " + buildSelect(), e);
        }
        return null;
    }

    @Override
    public <K> boolean remove(K key, Serializer<K> keySerializer) throws IOException {
        final byte[] keyBytes = serialize(key, keySerializer);
        try (Connection con = dbcpConnectionPool.getConnection()) {
            try (PreparedStatement stmt = con.prepareStatement(buildDelete())) {
                stmt.setBytes(1, keyBytes);
                int rowCount = stmt.executeUpdate();
                if (rowCount == 0) {
                    return false;
                }
            }
        } catch (SQLException e) {
            throw new IOException("Failed to Execute " + buildDelete(), e);
        }
        return true;
    }

    /**
     * There isn't a common way to implement regex across various RDBMS vendors.
     * In the future I'll implement SQL Dialects to handle this.
     **/
    @Override
    public long removeByPattern(String regex) throws IOException {
        throw new IOException("DBCP removeByPattern is not implemented");
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    protected void finalize() throws Throwable {
    }
}