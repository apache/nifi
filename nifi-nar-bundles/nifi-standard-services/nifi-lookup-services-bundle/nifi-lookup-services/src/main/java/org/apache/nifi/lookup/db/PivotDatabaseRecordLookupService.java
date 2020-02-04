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
import com.github.benmanes.caffeine.cache.Expiry;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import org.apache.nifi.util.Tuple;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Tags({"lookup", "cache", "enrich", "join", "rdbms", "database", "reloadable", "key", "value", "record"})
@CapabilityDescription("A relational-database-based lookup service. When the lookup key is found in the database, "
        + "the specified columns (or all if Lookup Value Columns are not specified) are returned as a Record. Only one row "
        + "will be returned for each lookup, duplicate database entries are ignored.")
public class PivotDatabaseRecordLookupService extends AbstractDatabaseLookupService implements RecordLookupService {

    private volatile Cache<Tuple<String, Object>, Record> cache;

    static final PropertyDescriptor LOOKUP_MAP_KEY_COLUMN =
            new PropertyDescriptor.Builder()
                    .name("lookup-map-key-column")
                    .displayName("Lookup Value Column")
                    .description("The column whose value will be returned when the Lookup value is matched")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                    .build();

    static final PropertyDescriptor LOOKUP_MAP_VALUE_COLUMN =
            new PropertyDescriptor.Builder()
                    .name("lookup-map-value-column")
                    .displayName("Lookup Value Column")
                    .description("The column whose value will be returned when the Lookup value is matched")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                    .build();

    static final PropertyDescriptor MAP_FIELD_NAME =
            new PropertyDescriptor.Builder()
                    .name("lookup-map-field-name")
                    .displayName("Map Field Name")
                    .description("The field name for the map in the result record")
                    .defaultValue("map")
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .build();

    @Override
    protected List<PropertyDescriptor> getValueProperties() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(LOOKUP_MAP_KEY_COLUMN);
        properties.add(LOOKUP_MAP_VALUE_COLUMN);
        properties.add(MAP_FIELD_NAME);

        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        this.lookupKeyColumn = context.getProperty(LOOKUP_KEY_COLUMN).evaluateAttributeExpressions().getValue();
        final int cacheSize = context.getProperty(CACHE_SIZE).evaluateAttributeExpressions().asInteger();
        final boolean clearCache = context.getProperty(CLEAR_CACHE_ON_ENABLED).asBoolean();
        final long durationNanos = context.getProperty(CACHE_EXPIRATION).isSet() ? context.getProperty(CACHE_EXPIRATION).evaluateAttributeExpressions().asTimePeriod(TimeUnit.NANOSECONDS) : 0L;
        if (this.cache == null || (cacheSize > 0 && clearCache)) {
            if (durationNanos > 0) {
                this.cache = Caffeine.newBuilder()
                        .maximumSize(cacheSize)
                        .expireAfter(new Expiry<Tuple<String, Object>, Record>() {
                            @Override
                            public long expireAfterCreate(Tuple<String, Object> stringObjectTuple, Record record, long currentTime) {
                                return durationNanos;
                            }

                            @Override
                            public long expireAfterUpdate(Tuple<String, Object> stringObjectTuple, Record record, long currentTime, long currentDuration) {
                                return currentDuration;
                            }

                            @Override
                            public long expireAfterRead(Tuple<String, Object> stringObjectTuple, Record record, long currentTime, long currentDuration) {
                                return currentDuration;
                            }
                        })
                        .build();
            } else {
                this.cache = Caffeine.newBuilder()
                        .maximumSize(cacheSize)
                        .build();
            }
        }
    }

    @Override
    protected String sqlSelectList(Map<String, String> context) {
        String keyColumn = getProperty(LOOKUP_MAP_KEY_COLUMN).evaluateAttributeExpressions(context).getValue();
        String valueColumn = getProperty(LOOKUP_MAP_VALUE_COLUMN).evaluateAttributeExpressions(context).getValue();
        return String.format("%s, %s", keyColumn, valueColumn);
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

        final String sql = buildSQLStatement(context);
        List<Object> keys = getCoordinates(coordinates);

        final String mapFieldName = getProperty(MAP_FIELD_NAME).evaluateAttributeExpressions().getValue();

        if (keys.isEmpty()) {
            return Optional.empty();
        }

        Tuple<String, Object> cacheLookupKey = new Tuple<>(sql, keys.hashCode());

        // Not using the function param of cache.get so we can catch and handle the checked exceptions
        Record foundRecord = cache.get(cacheLookupKey, k -> null);

        if (foundRecord == null) {
            final String selectQuery = buildSQLStatement(context);
            try (final Connection con = dbcpService.getConnection(context);
                 final PreparedStatement st = con.prepareStatement(selectQuery)) {

                int iterPos = 1;
                for (Object key : keys) {
                    st.setObject(iterPos, key);
                    iterPos++;
                }
                ResultSet resultSet = st.executeQuery();

                ResultSetRecordSet resultSetRecordSet = new ResultSetRecordSet(resultSet, null);

                DataType valueDataType = null;
                Map<String, Object> result = new HashMap<>();
                Map<String, Object> mapValues = new HashMap<>();
                Set<String> uniqueKeys = new HashSet<>();
                while((foundRecord = resultSetRecordSet.next()) != null) {
                    if (valueDataType == null) {
                        valueDataType = foundRecord.getSchema().getField(1).getDataType();
                    }
                    String keyValue = foundRecord.getAsString(foundRecord.getSchema().getField(0).getFieldName());
                    if (!uniqueKeys.add(keyValue))
                        throw new LookupFailureException(String.format("Duplicate row for key column \"%s\".", keyValue));
                    Object valueValue = foundRecord.getValues()[1];

                    mapValues.put(keyValue, valueValue);
                }

                if (mapValues.isEmpty()) {
                    return Optional.empty();
                } else {
                    List<RecordField> recordFields = new ArrayList<>();
                    recordFields.add(new RecordField(mapFieldName, RecordFieldType.MAP.getMapDataType(valueDataType)));
                    RecordSchema schema = new SimpleRecordSchema(recordFields);
                    result.put(mapFieldName, mapValues);
                    foundRecord = new MapRecord(schema, result);

                    cache.put(cacheLookupKey, foundRecord);
                }

            } catch (SQLException se) {
                String commaSeparatedKeys = keys
                        .stream()
                        .map(String::valueOf)
                        .collect(Collectors.joining(","));
                throw new LookupFailureException("Error executing SQL statement: " + selectQuery + "for values " + commaSeparatedKeys
                        + " : " + (se.getCause() == null ? se.getMessage() : se.getCause().getMessage()), se);
            } catch (IOException ioe) {
                String commaSeparatedKeys = keys
                        .stream()
                        .map(String::valueOf)
                        .collect(Collectors.joining(","));
                throw new LookupFailureException("Error retrieving result set for SQL statement: " + selectQuery + "for values " + commaSeparatedKeys
                        + " : " + (ioe.getCause() == null ? ioe.getMessage() : ioe.getCause().getMessage()), ioe);
            }
        }

        return Optional.ofNullable(foundRecord);
    }

    @Override
    public Set<String> getRequiredKeys() {
        return Collections.emptySet();
    }
}
