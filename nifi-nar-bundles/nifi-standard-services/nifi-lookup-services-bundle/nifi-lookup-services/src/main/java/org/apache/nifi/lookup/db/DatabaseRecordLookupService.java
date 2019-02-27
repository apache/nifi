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
package org.apache.nifi.lookup.db;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import org.apache.nifi.util.Tuple;
import org.apache.nifi.util.db.JdbcCommon;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Tags({"lookup", "cache", "enrich", "join", "rdbms", "database", "reloadable", "key", "value", "record"})
@CapabilityDescription("A relational-database-based lookup service. When the lookup key is found in the database, " +
        "the columns are returned as a Record. Only one row will be returned for each lookup, duplicate database entries are ignored.")
public class DatabaseRecordLookupService extends AbstractControllerService implements RecordLookupService {

    private static final String KEY = "key";

    private static final Set<String> REQUIRED_KEYS = Collections.unmodifiableSet(Stream.of(KEY).collect(Collectors.toSet()));

    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-dbcp-service")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-table-name")
            .displayName("Table Name")
            .description("The name of the database table to be queried. Note that this may be case-sensitive depending on the database.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor LOOKUP_KEY_COLUMN = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-key column")
            .displayName("Lookup Key Column")
            .description("The column in the table that will serve as the lookup key. This is the column that will be matched against "
                    + "the property specified in the lookup processor. Note that this may be case-sensitive depending on the database.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-cache-size")
            .displayName("Cache Size")
            .description("Specifies how many lookup values/records should be cached. The cache is shared for all tables and keeps a map of lookup values to records. "
                    + "Setting this property to zero means no caching will be done and the table will be queried for each lookup value in each record. If the lookup "
                    + "table changes often or the most recent data must be retrieved, do not use the cache.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .defaultValue("0")
            .required(true)
            .build();

    static final PropertyDescriptor CLEAR_CACHE_ON_ENABLED = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-clear-cache-on-enabled")
            .displayName("Clear Cache on Enabled")
            .description("Whether to clear the cache when this service is enabled. If the Cache Size is zero then this property is ignored. Clearing the cache when the "
                    + "service is enabled ensures that the service will first go to the database to get the most recent data.")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .required(true)
            .build();

    private List<PropertyDescriptor> properties;

    private volatile Cache<Tuple<String, Object>, Record> cache;

    protected DBCPService dbcpService;

    private volatile String lookupKeyColumn;

    private volatile JdbcCommon.AvroConversionOptions options;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DBCP_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(LOOKUP_KEY_COLUMN);
        properties.add(CACHE_SIZE);
        properties.add(CLEAR_CACHE_ON_ENABLED);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        this.lookupKeyColumn = context.getProperty(LOOKUP_KEY_COLUMN).evaluateAttributeExpressions().getValue();
        int cacheSize = context.getProperty(CACHE_SIZE).evaluateAttributeExpressions().asInteger();
        boolean clearCache = context.getProperty(CLEAR_CACHE_ON_ENABLED).asBoolean();
        if(this.cache == null || (cacheSize > 0 && clearCache)) {
            this.cache = Caffeine.newBuilder()
                    .maximumSize(cacheSize)
                    .build();
        }

        options = JdbcCommon.AvroConversionOptions.builder()
                .recordName("NiFi_DB_Record_Lookup")
                // Ignore duplicates
                .maxRows(1)
                // Keep column names as field names
                .convertNames(false)
                .useLogicalTypes(true)
                .build();
    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        return lookup(coordinates, null);
    }

    @Override
    public Optional<Record> lookup(final Map<String, Object> coordinates, Map<String, String> context) throws LookupFailureException {
        if (coordinates == null) {
            return Optional.empty();
        }

        final Object key = coordinates.get(KEY);
        if (StringUtils.isBlank(key.toString())) {
            return Optional.empty();
        }

        final String tableName = getProperty(TABLE_NAME).evaluateAttributeExpressions(context).getValue();

        Tuple<String, Object> cacheLookupKey = new Tuple<>(tableName, key);

        // Not using the function param of cache.get so we can catch and handle the checked exceptions
        Record foundRecord = cache.get(cacheLookupKey, k -> null);

        if (foundRecord == null) {
            final String selectQuery = "SELECT * FROM " + tableName + " WHERE " + lookupKeyColumn + " = ?";
            try (final Connection con = dbcpService.getConnection(context);
                 final PreparedStatement st = con.prepareStatement(selectQuery)) {

                st.setObject(1, key);
                ResultSet resultSet = st.executeQuery();
                final Schema avroSchema = JdbcCommon.createSchema(resultSet, options);
                final RecordSchema recordAvroSchema = AvroTypeUtil.createSchema(avroSchema);
                ResultSetRecordSet resultSetRecordSet = new ResultSetRecordSet(resultSet, recordAvroSchema, true);
                foundRecord = resultSetRecordSet.next();

                // Populate the cache if the record is present
                if (foundRecord != null) {
                    lock.writeLock().lock();
                    cache.put(cacheLookupKey, foundRecord);
                    lock.writeLock().unlock();
                }

            } catch (SQLException se) {
                throw new LookupFailureException("Error executing SQL statement: " + selectQuery + "for value " + key.toString(), se);
            } catch (IOException ioe) {
                throw new LookupFailureException("Error retrieving result set for SQL statement: " + selectQuery + "using value " + key.toString(), ioe);
            }
        }

        return Optional.ofNullable(foundRecord);
    }


    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }
}
