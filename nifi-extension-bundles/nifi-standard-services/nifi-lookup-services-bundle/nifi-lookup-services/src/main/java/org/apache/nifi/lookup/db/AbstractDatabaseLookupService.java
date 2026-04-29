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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AbstractDatabaseLookupService extends AbstractControllerService {

    static final String KEY = "key";

    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the database table to be queried. Note that this may be case-sensitive depending on the database.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor LOOKUP_KEY_COLUMN = new PropertyDescriptor.Builder()
            .name("Lookup Key Column")
            .description("The column(s) in the table that will serve as the lookup key. For composite key lookups, "
                    + "specify a comma-separated list of column names (e.g. 'col1, col2'). When a single column is specified, "
                    + "the lookup processor should pass a coordinate named 'key'. When multiple columns are specified, "
                    + "the lookup processor should pass coordinates whose names match the column names. "
                    + "Note that this may be case-sensitive depending on the database.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
            .name("Cache Size")
            .description("Specifies how many lookup values/records should be cached. The cache is shared for all tables and keeps a map of lookup values to records. "
                    + "Setting this property to zero means no caching will be done and the table will be queried for each lookup value in each record. If the lookup "
                    + "table changes often or the most recent data must be retrieved, do not use the cache.")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .defaultValue("0")
            .required(true)
            .build();

    static final PropertyDescriptor CLEAR_CACHE_ON_ENABLED = new PropertyDescriptor.Builder()
            .name("Clear Cache on Enabled")
            .description("Whether to clear the cache when this service is enabled. If the Cache Size is zero then this property is ignored. Clearing the cache when the "
                    + "service is enabled ensures that the service will first go to the database to get the most recent data.")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("true")
            .required(true)
            .build();

    static final PropertyDescriptor CACHE_EXPIRATION = new PropertyDescriptor.Builder()
            .name("Cache Expiration")
            .description("Time interval to clear all cache entries. If the Cache Size is zero then this property is ignored.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    protected List<PropertyDescriptor> properties;

    DBCPService dbcpService;

    volatile String lookupKeyColumn;

    volatile List<String> lookupKeyColumns;

    static List<String> parseKeyColumns(final String value) {
        if (value == null || value.isBlank()) {
            return Collections.emptyList();
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    boolean isCompositeKey() {
        return lookupKeyColumns != null && lookupKeyColumns.size() > 1;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.renameProperty("dbrecord-lookup-dbcp-service", DBCP_SERVICE.getName());
        config.renameProperty("dbrecord-lookup-table-name", TABLE_NAME.getName());
        config.renameProperty("dbrecord-lookup-key-column", LOOKUP_KEY_COLUMN.getName());
        config.renameProperty("dbrecord-lookup-cache-size", CACHE_SIZE.getName());
        config.renameProperty("dbrecord-lookup-clear-cache-on-enabled", CLEAR_CACHE_ON_ENABLED.getName());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
}
